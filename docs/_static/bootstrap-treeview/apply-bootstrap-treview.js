$(document).ready(function() {
    // apply the class view hierarchy
    $("#class-treeView").treeview({
        data: getClassViewTree(),
        enableLinks: true
    });

    // apply the directory view hierarchy
    $("#directory-treeView").treeview({
        data: getDirectoryViewTree(),
        enableLinks: true
    });
});
