
SELECT rule_id, LEFT(embedding::text, 60) AS embedding_preview
FROM core.bank_rulebook

SELECT vendor_id, COUNT(category_id) AS rule_count,cOUNT(DISTINCT RULE_ID) AS unique_rule_count
FROM core.bank_rulebook
GROUP BY vendor_id;

SELECT DISTINCT vendor_id
FROM core.bank_rulebook;

SELECT category_code, count(rule_id) AS rule_count
--rule_id, rule_title, bian_domain, category_code
FROM core.bank_rulebook
WHERE vendor_id = 'SBI Card'
GROUP BY category_code;

SELECT category_code, count(rule_id) AS rule_count
--rule_id, rule_title, bian_domain, category_code
FROM core.bank_rulebook
WHERE vendor_id LIKE '%HDFC%'
GROUP BY category_code;