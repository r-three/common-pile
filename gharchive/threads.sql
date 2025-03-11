SELECT
    -- These are the same for all comments that are grouped together.
    ANY_VALUE(repo_name) as repo_name,
    ANY_VALUE(issue_id) as thread_id,
    ANY_VALUE(issue_title) as thread_title,
    ANY_VALUE(issue_body) as thread_body,
    ANY_VALUE(issue_username) as thread_author_username,
    ANY_VALUE(issue_ts) as thread_timestamp,
    ANY_VALUE(url) as thread_url,
    -- Things like ARRAY_AGG, seem to require a lot more overhead to save so you are
    --   more likely to hit the 100MB record limit.
    -- Similarly, we limit to 250 comments to avoid this limit. The most comments I saw
    --   when spot checking another query was ~256, but the query does crash without this
    --   limit.
    STRING_AGG(username, "⇭⇭⇭" ORDER BY comment_ts LIMIT 250) as comment_author_username,
    STRING_AGG(comment_body, "⇭⇭⇭" ORDER BY comment_ts LIMIT 250) as comment_body,
    STRING_AGG(comment_ts, "⇭⇭⇭" ORDER BY comment_ts LIMIT 250) as comment_timestamp,
FROM (
  SELECT
    -- IssueCommentEvents also have a type field that should be created, edited, or deleted
    --   but queries I ran looking for edited comments never returned anything. Seems there
    --   is some issue with edited events so we are just including these comments as they are
    --   in the table which seems to be exclusively "created".
    repo.name as repo_name,
    JSON_EXTRACT(payload, '$.issue.id') as issue_id,
    JSON_EXTRACT(payload, '$.issue.user.login') as issue_username,
    JSON_EXTRACT(payload, '$.issue.title') as issue_title,
    JSON_EXTRACT(payload, '$.issue.body') as issue_body,
    JSON_EXTRACT(payload, '$.issue.created_at') as issue_ts,
    JSON_EXTRACT(payload, '$.issue.html_url') as url,
    IFNULL(JSON_EXTRACT(payload, '$.comment.user.login'), "") as username,
    IFNULL(JSON_EXTRACT(payload, '$.comment.user.html_url'), "") as user_url,
    IFNULL(JSON_EXTRACT(payload, '$.comment.body'), "") as comment_body,
    IFNULL(JSON_EXTRACT(payload, '$.comment.updated_at'), "") as comment_ts
  FROM `githubarchive.year.*`
  WHERE type = 'IssueCommentEvent'
  -- We order groups (by the issue_id) based on the comment_ts before the aggregation (STRING_AGG)
  --   so we get the comments in the correct order, but not have to sort this whole table (which
  --   resulted in OoM issues).
  -- ORDER BY comment_ts
)
-- Simple filtering of bots.
WHERE
  (NOT ENDS_WITH(username, "-[bot]"))
  AND (NOT ENDS_WITH(username, "-bot"))
  AND (NOT ENDS_WITH(issue_username, "-[bot]"))
  AND (NOT ENDS_WITH(issue_username, "-bot"))
GROUP BY issue_id
