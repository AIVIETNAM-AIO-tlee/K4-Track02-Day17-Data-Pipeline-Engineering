-- Events are immutable facts: insert the ones never seen before (append + dedup),
-- no update path needed. `event_time` lets microbatch models filter this table.
{{ config(
    incremental_strategy='append',
    event_time='event_time'
) }}

select event_id, user_id, ticket_id, type, rating, page, event_time, _ingested_at, _batch_id
from {{ ref('stg_events') }} as s
{% if is_incremental() %}
where not exists (select 1 from {{ this }} as t where t.event_id = s.event_id)
{% endif %}
qualify row_number() over (partition by event_id order by _ingested_at, _kafka_offset) = 1
