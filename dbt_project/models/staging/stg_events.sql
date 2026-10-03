-- Kafka event records, typed + the same quality rules as pipeline/quality.py.
-- Rows that fail are simply not selected here; the lite path quarantines them.
with src as (
    select _payload::json->'value' as v, _ingested_at, _batch_id, _kafka_partition, _kafka_offset
    from {{ source('bronze', 'events') }}
),
typed as (
    select
        v->>'event_id'                                              as event_id,
        v->>'user_id'                                               as user_id,
        v->>'ticket_id'                                             as ticket_id,
        v->>'type'                                                  as type,
        v->>'rating'                                                as rating,
        v->>'page'                                                  as page,
        cast(replace(v->>'event_time', 'Z', '') as timestamp)      as event_time,
        _ingested_at, _batch_id, _kafka_partition, _kafka_offset
    from src
)
select * from typed
where coalesce(event_id, '') <> ''
  and coalesce(user_id, '') <> ''
  and event_time is not null
  and (   (type = 'click'    and rating is null)
       or (type = 'feedback' and rating in ('up', 'down')))
