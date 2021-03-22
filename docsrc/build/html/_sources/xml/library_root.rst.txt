
Library API
===========

.. include:: class_view_hierarchy.rst

.. include:: file_view_hierarchy.rst

.. include:: unabridged_api.rst



.. raw:: html

   <script type="text/javascript">
       /* NOTE: if you are reading this, Exhale generated this directly. */
       $(document).ready(function() {
           /* Inspired by very informative answer to get color of links:
              https://stackoverflow.com/a/2707837/3814202 */
           var $fake_link = $('<a href="#"></a>').hide().appendTo("body");
           var linkColor = $fake_link.css("color");
           $fake_link.remove();

           var $fake_p = $('<p class="text-muted"></p>').hide().appendTo("body");
           var iconColor = $fake_p.css("color");
           $fake_p.remove();

           /* After much deliberation, using JavaScript directly to enforce that the
            * link and glyphicon receive different colors is fruitless, because the
            * bootstrap treeview library will overwrite the style every time.  Instead,
            * leaning on the library code itself to append some styling to the head,
            * I choose to mix a couple of things:
            *
            * 1. Set the `color` property of bootstrap treeview globally, this would
            *    normally affect the color of both the link text and the icon.
            * 2. Apply custom forced styling of the glyphicon itself in order to make
            *    it a little more clear to the user (via different colors) that the
            *    act of clicking the icon and the act of clicking the link text perform
            *    different actions.  The icon expands, the text navigates to the page.
            */
            // Part 1: use linkColor as a parameter to bootstrap treeview

            // apply the class view hierarchy
            $("#class-treeView").treeview({
                data: getClassHierarchyTree(),
                enableLinks: true,
                color: linkColor,
                showTags: true,
                collapseIcon: "glyphicon glyphicon-minus",
                expandIcon: "glyphicon glyphicon-plus",
                levels: 1,
                onhoverColor: "#F5F5F5"
            });

            // apply the file view hierarchy
            $("#file-treeView").treeview({
                data: getFileHierarchyTree(),
                enableLinks: true,
                color: linkColor,
                showTags: true,
                collapseIcon: "glyphicon glyphicon-minus",
                expandIcon: "glyphicon glyphicon-plus",
                levels: 1,
                onhoverColor: "#F5F5F5"
            });

            // Part 2: override the style of the glyphicons by injecting some CSS
            $('<style type="text/css" id="exhaleTreeviewOverride">' +
              '    .treeview span[class~=icon] { '                 +
              '        color: ' + iconColor + ' ! important;'       +
              '    }'                                              +
              '</style>').appendTo('head');
       });
   </script>
