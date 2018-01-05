view: redshift_wlm_queries {
    derived_table: {
      sql: SELECT
            w.query, "substring"(q.querytxt, 1, 4000) AS querytxt
          , w.queue_start_time
          , w.service_class
          , case when w.service_class = 1 then 'system-health'
                 when w.service_class = 2 then 'system-metrics'
                 when w.service_class = 3 then 'system-cmstats'
                 when w.service_class = 4 then 'system'
                 when w.service_class = 6 then 'dw_reporting'
                 when w.service_class = 7 then 'dw_readwrite'
                 when w.service_class = 8 then 'dw_default'
                 when w.service_class = 14 then 'short_query_queue'
                 else coalesce(trim(b.condition),w.service_class::text) end as queue_name
          , w.slot_count AS slots, w.total_queue_time / 1000000 AS queue_seconds
          , w.total_exec_time / 1000000 AS exec_seconds
          , (w.total_queue_time + w.total_exec_time) / 1000000 AS total_seconds
            FROM stl_wlm_query w
            JOIN stv_wlm_classification_config b ON w.service_class = b.action_service_class
            LEFT JOIN stl_query q ON q.query = w.query AND q.userid = w.userid
            WHERE w.queue_start_time >= date_add('day', -7, 'now')
            AND q.starttime >= date_add('day', -7, 'now')
            AND w.userid > 1 ;;
    }

  dimension: query {
    type:  number
    hidden: yes
    primary_key: yes
    sql: ${TABLE}.query ;;
  }

  dimension: query_txt {
    type:  string
    sql: ${TABLE}.querytxt ;;
  }

  dimension_group: queue_start_time {
    type: time
    timeframes: [
      raw,
      date,
      time,
      hour12,
      hour,
      minute30,
      minute15,
      minute5,
      minute
    ]
    sql: ${TABLE}.queue_start_time ;;
  }

  dimension: queue_name {
    type:  string
    sql: ${TABLE}.queue_name ;;
    suggestions: ["dw_reporting", "dw_readwrite", "dw_default", "short_query_queue"]
  }

  dimension: queue_id {
    type:  string
    sql: ${TABLE}.service_class ;;
  }

  dimension: queue_seconds {
    type:  number
    sql: ${TABLE}.queue_seconds ;;
  }

  dimension: exec_seconds {
    type:  number
    sql: ${TABLE}.exec_seconds ;;
  }

  dimension: total_seconds {
    type:  number
    sql: ${TABLE}.total_seconds ;;
  }


  # ----- Sets of fields for drilling ------
  set: query_detail {
    fields: [
      query_txt,
      queue_start_time_time,
      queue_name,
      queue_id,
      queue_seconds,
      exec_seconds,
      total_seconds
    ]
  }



  measure: count_queries {
    type: count
    drill_fields: [query_detail*]
  }

  measure: sum_queue_seconds {
    type: sum
    sql: ${queue_seconds} ;;
    drill_fields: [query_detail*]
  }

  measure: sum_exec_seconds {
    type: sum
    sql: ${exec_seconds} ;;
    drill_fields: [query_detail*]
  }

  measure: sum_total_seconds {
    type: sum
    sql: ${total_seconds} ;;
    drill_fields: [query_detail*]
  }

  measure: mean_queue_seconds {
    type: average
    sql: ${queue_seconds} ;;
    drill_fields: [query_detail*]
  }

  measure: mean_exec_seconds {
    type: average
    sql: ${exec_seconds} ;;
    drill_fields: [query_detail*]
  }

  measure: mean_total_seconds {
    type: sum
    sql: ${total_seconds} ;;
    drill_fields: [query_detail*]
  }

  measure: median_queue_seconds {
    type: median
    sql: ${queue_seconds} ;;
    drill_fields: [query_detail*]
  }

  measure: median_exec_seconds {
    type: median
    sql: ${exec_seconds} ;;
    drill_fields: [query_detail*]
  }

  measure: median_total_seconds {
    type: median
    sql: ${total_seconds} ;;
    drill_fields: [query_detail*]
  }

  measure: top_n_execution {
    type: number
    sql:  row_number() over (order by exec_seconds desc) ;;
  }

}
