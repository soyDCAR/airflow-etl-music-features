with source as (
    select * from {{ source('raw', 'audio_features') }}
),
enriched as (
    select
        track_id,
        coalesce(title, 'Unknown')   as title,
        duration_sec,
        sample_rate,
        mfcc_mean,
        mfcc_std,
        spectral_centroid_mean,
        spectral_centroid_std,
        tempo,
        case
            when tempo < 60  then 'very_slow'
            when tempo < 90  then 'slow'
            when tempo < 120 then 'moderate'
            when tempo < 150 then 'fast'
            else                  'very_fast'
        end                          as tempo_bucket,
        case
            when duration_sec < 60  then 'short'
            when duration_sec < 180 then 'medium'
            else                         'long'
        end                          as duration_bucket,
        processed_at,
        date_trunc('month', processed_at) as processed_month,
        created_at
    from source
    where track_id is not null
)
select * from enriched
