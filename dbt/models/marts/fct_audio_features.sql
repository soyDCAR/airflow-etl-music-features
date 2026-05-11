{{
    config(
        materialized='incremental',
        unique_key='track_id',
        on_schema_change='sync_all_columns'
    )
}}
with stg as (
    select * from {{ ref('stg_audio_features') }}
    {% if is_incremental() %}
    where processed_at > (select max(processed_at) from {{ this }})
    {% endif %}
)
select
    track_id,
    duration_sec,
    sample_rate,
    mfcc_mean,
    mfcc_std,
    spectral_centroid_mean,
    spectral_centroid_std,
    tempo,
    tempo_bucket,
    duration_bucket,
    processed_month,
    processed_at
from stg
