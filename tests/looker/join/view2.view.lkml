view: view2 {
  sql_table_name: schema2.view2 ;;

  dimension: country {
    type: string
    description: "The country"
    sql: ${TABLE}.country ;;
  }

  measure: average_measurement {
    group_label: "Measurement"
    type: average
    description: "My measurement"
    sql: ${TABLE}.measurement ;;
  }

}