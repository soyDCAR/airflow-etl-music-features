with stg as (select * from {{ ref('stg_audio_features') }})
select
    track_id,
    title,
    sample_rate,
    tempo_bucket,
    duration_bucket,
    processed_at   as first_processed_at,
    processed_month as first_processed_month
from stg
