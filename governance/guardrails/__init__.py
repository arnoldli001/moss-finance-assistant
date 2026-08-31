# governance/guardrails: 输入输出安全护栏
#   输入侧：prompt_sanitizer(Prompt注入清洗) / rbac(基于 rbac_policy.json 的角色权限)
#   过程侧：circuit_breaker(熔断三态) / error_classifier(错误四象限) / degradation_chain(四级降级链) / hallucination_guard(幻觉防护) / semantic_cache
#   输出侧：maker_checker(Maker-Checker双校验) / output_validator(风险声明、格式、长度、XSS校验)
