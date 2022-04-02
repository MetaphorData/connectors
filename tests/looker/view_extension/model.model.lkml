connection: "bigquery"

# simple extends
view: view1 {
  extends: [base_view1]
}

# multiple extends
view: view2 {
  extends: [base_view1, base_view2]
}

# chained extends
view: view3 {
  extends: [view2]
}

# view that requires extension
view: base_view1 {
  sql_table_name: table1 ;;
  extension: required
}

view: base_view2 {
  sql_table_name: table2 ;;
}

explore: explore1 {
  view_name: "view1"
}
