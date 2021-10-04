connection: "snowflake"

include: "/*.view"

explore: explore1 {
  label: "label"
  description: "description"
  view_name: "view1"

  join: view2 {
    type: left_outer
    relationship: many_to_one
    sql_on: ${view2.country} = ${view1.country} ;;
  }

  join: view1_again {
    from: view1
    type: left_outer
    relationship: one_to_one
    sql_on: ${view2.country} = ${view1.country} ;;
  }

}