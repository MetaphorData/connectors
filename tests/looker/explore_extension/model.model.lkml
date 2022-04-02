connection: "bigquery"

# simple extends
explore: explore1 {
  extends: [base_explore1]
}

# explore that requires extension
explore: base_explore1 {
  view_name: "view1"
  extension: required
}

view: view1 {
  sql_table_name: table1 ;;
}
