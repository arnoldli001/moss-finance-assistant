import logging
import os
from pathlib import Path
import time

try:
    import markdown
    import win32com.client
    import pythoncom
except ImportError:
    pass

# 项目根目录：对外消息里把绝对路径转成相对路径，避免暴露服务器磁盘结构
_PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _rel_display(p) -> str:
    """返回对外展示的路径：项目根下走相对路径；之外保留文件名。"""
    try:
        pp = Path(p)
        return str(pp.resolve().relative_to(_PROJECT_ROOT)).replace("\\", "/")
    except Exception:
        return Path(p).name or str(p)


def convert_md_to_pdf_via_word(md_abs_path: Path, pdf_abs_path: Path) -> str:
    """
    使用 Microsoft Word COM 接口将 Markdown 转换为 PDF。
    依赖：pywin32, markdown
    """
    temp_html_path = md_abs_path.with_suffix('.temp.html')
    word_app = None

    try:
        # 1. MD 转 HTML
        with open(md_abs_path, 'r', encoding='utf-8') as f:
            md_content = f.read()

        html_body = markdown.markdown(md_content, extensions=['tables', 'fenced_code'])
        html_content = f"""
        <html>
        <head>
            <meta charset="UTF-8">
            <style>
                body {{ font-family: "Microsoft YaHei", "SimHei", sans-serif; }}
                table {{ border-collapse: collapse; width: 100%; }}
                th, td {{ border: 1px solid black; padding: 8px; }}
                pre {{ background-color: #f5f5f5; padding: 10px; border-radius: 4px; }}
                code {{ font-family: "Consolas", "Monaco", monospace; }}
            </style>
        </head>
        <body>
            {html_body}
        </body>
        </html>
        """

        with open(temp_html_path, 'w', encoding='utf-8') as f:
            f.write(html_content)

        # 2. 调用 Word COM
        pythoncom.CoInitialize()
        word_app = win32com.client.Dispatch('Word.Application')
        word_app.Visible = False
        word_app.DisplayAlerts = False

        doc = word_app.Documents.Open(str(temp_html_path.resolve()))
        doc.SaveAs(str(pdf_abs_path.resolve()), FileFormat=17)  # wdFormatPDF = 17
        doc.Close(SaveChanges=0)

        # 🔒 对外展示使用相对路径（不暴露服务器 D:\xxx 绝对路径）
        pdf_rel = _rel_display(pdf_abs_path)
        if pdf_abs_path.exists():
            return f"成功转换: {pdf_rel} (Word引擎)"
        else:
            return f"转换完成但未生成文件: {pdf_rel}"

    except ImportError:
        return "缺少依赖库，请安装: pip install pywin32 markdown"
    except Exception as e:
        logging.error(f"Word转换PDF失败: {e}", exc_info=True)
        return f"转换失败: {type(e).__name__}"

    finally:
        # 3. 资源清理
        if word_app:
            try:
                word_app.Quit()
            except:
                pass

        if temp_html_path.exists():
            try:
                temp_html_path.unlink()
            except:
                pass

        try:
            pythoncom.CoUninitialize()
        except:
            pass