view: view1 {
  derived_table: {
    sql:
      SELECT * FROM table1 ;;
  }

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

view: view2 {
  derived_table: {
    sql:
      SELECT * FROM ${view1.SQL_TABLE_NAME} ;;
  }

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

view: view3 {
  derived_table: {
    explore_source: view1 {
      column: country {
        field: view1.country
      }
    }
  }

  dimension: country {
    type: string
    description: "The country"
    sql: ${TABLE}.country ;;
  }
}