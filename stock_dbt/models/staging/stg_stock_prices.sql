{{
  config(
    materialized = 'view',
    schema = 'staging'
  )
}}

with source as (

    select * from {{ source('raw', 'stock_prices') }}

),

renamed as (

    select
        -- Keys
        date::date                              as date,
        upper(trim(ticker))                     as ticker,

        -- Prices
        open::numeric(14,4)                     as open,
        high::numeric(14,4)                     as high,
        low ::numeric(14,4)                     as low,
        close::numeric(14,4)                    as close,
        coalesce(
            adjusted_close,
            close
        )::numeric(14,4)                        as adjusted_close,

        -- Volume
        volume::bigint                          as volume,

        -- Audit
        ingested_at                             as ingested_at

    from source

    where
        date is not null
        and ticker is not null
        and close is not null

)

select * from renamed
