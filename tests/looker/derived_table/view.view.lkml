view: view1 {
  derived_table: {
    sql:
      SELECT * FROM table1 ;;
  }
}

view: view2 {
  derived_table: {
    sql:
      SELECT * FROM ${view1.SQL_TABLE_NAME} ;;
  }
}

view: view3 {
  derived_table: {
    explore_source: explore1 {
      
    }
  }
}