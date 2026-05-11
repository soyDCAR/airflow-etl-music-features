with fct as (select * from {{ ref('fct_audio_features') }})
select
    tempo_bucket,
    count(*)                                                          as track_count,
    round(avg(tempo)::numeric, 2)                                     as avg_tempo_bpm,
    round(min(tempo)::numeric, 2)                                     as min_tempo_bpm,
    round(max(tempo)::numeric, 2)                                     as max_tempo_bpm,
    round(stddev(tempo)::numeric, 2)                                  as stddev_tempo_bpm,
    round(avg(spectral_centroid_mean)::numeric, 2)                    as avg_spectral_centroid_hz,
    round(avg(duration_sec)::numeric, 2)                              as avg_duration_sec,
    round(count(*) * 100.0 / nullif(sum(count(*)) over (), 0), 1)     as pct_of_total
from fct
group by tempo_bucket
order by avg_tempo_bpm
