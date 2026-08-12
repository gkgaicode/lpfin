Let's tackle both exercises back-to-back. We will start with the Live SQL Performance Tuning Challenge to lock down your technical edge, followed immediately by the Leadership STAR Story Refinement.
------------------------------
## Part 1: The Live SQL Performance Tuning Challenge## 🚨 The Production Bottleneck

"Every morning at 6:00 AM, a critical analytical query runs inside Snowflake to aggregate client portfolio histories for LPL’s compliance reporting dashboard. Over the last three months, as client accounts grew, the execution time for this query spiked from 12 minutes to 2.5 hours, delaying executive reports. The database administrator pulled the raw query execution plan and flagged massive Data Spilling to Remote Storage and a Cartesian Join Bottleneck."

Here is the current unoptimized query written by a mid-level engineer:

SELECT 
    c.account_id,
    c.client_name,
    t.trade_id,
    t.trade_amount,
    t.transaction_date,
    SUM(t.trade_amount) OVER (PARTITION BY c.client_name ORDER BY t.transaction_date) as running_total_by_clientFROM lpl_production.gold.clients cJOIN lpl_production.gold.transactions t 
    ON c.account_id LIKE concat('%', t.account_id, '%')WHERE DATE(t.transaction_date) >= DATEADD(day, -30, CURRENT_DATE())
  AND c.status = 'ACTIVE';

## 🔍 Your Task as the Tech Lead:
Analyze this query. Identify the three structural flaws causing the performance degradation, and write the optimized, production-grade version of this query.
(Take a moment to review the join condition, the window function, and the WHERE clause filtering. Reply with your diagnosis and your optimized SQL block, and we will evaluate it against Snowflake best practices.)
------------------------------
## Part 2: Leadership STAR Story Refinement
At your 14+ year AVP level, your behavioral answers must show high emotional intelligence, financial risk awareness, and clear technical authority.
To refine your story, please paste a rough, bulleted outline of a real situation from your past career using this framework:

* Situation: What was the high-stakes problem or project? (e.g., A failing system, a massive outage, or a tight deadline).
* Task: What was your specific responsibility as the leader?
* Action: What exact steps did you take to fix it? (Focus on architecture changes, team delegation, or stakeholder negotiation).
* Result: What was the quantitative outcome? (e.g., Saved $50k in cloud costs, reduced latency by 80%, or delivered 2 weeks early).

------------------------------
Whenever you are ready, drop your SQL optimizations and your rough STAR story notes below, and we will tune them both to perfection for your interview!
