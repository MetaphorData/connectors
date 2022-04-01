# include using absolute path
include: "/view2.view"

# include using relative path with file extension
include: "nested/view3.view.lkml"

# circular include (view1 => view4 => view1)
include: "../view4.view"

view: view1 {
}