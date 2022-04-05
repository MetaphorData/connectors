connection: "bigquery"

# simple extends
view: view1 {
  extends: [base_view1]
}

# multiple extends
view: view2 {
  extends: [base_view1, base_view2, base_view3]
}

# chained extends
view: view3 {
  extends: [view2]
}

# extends with override
view: view4 {
  extends: [base_view1]
  sql_table_name: table3 ;;
}

# view that requires extension
view: base_view1 {
  sql_table_name: table1 ;;
  extension: required
}

view: base_view2 {
  derived_table: {
    sql:
      SELECT * FROM table2 ;;
  }
}

view: base_view3 {
  derived_table: {
  }
}

explore: explore1 {
  view_name: "view1"
}
