connection: "bigquery"

# simple extension
explore: explore1 {
  extends: [base_explore1]
}

# multiple extensions
explore: explore2 {
  extends: [base_explore1, base_explore2, view3]
}

# chained extensions
explore: explore3 {
  extends: [explore1]
}

# extends with overrides
explore: explore4 {
  extends: [explore1]
  view_name: "view3"
}

# explore that requires extension
explore: base_explore1 {
  view_name: "view1"
  extension: required
}

# explore with "from"
explore: base_explore2 {
  from: "view2"
}

# explore without "from" or "view_name" 
explore: view3 {
}

view: view1 {
}

view: view2 {
}

view: view3 {
}