.. _webinars:

============================
GPU Accelerated Data Science
============================

We engage with the community by organizing hands-on webinars in both English and Spanish. Check
the sections below for a list of **Webinars delivered** and **Webinars planned**.


.. _webinars_planned:

Webinars planned
----------------

We aim to publish new content regularly! Check the list below for the 
list of all planned webinars. Also, you can 
`subscribe to our webinars calendar <https://calendar.google.com/calendar/u/0?cid=Y183YXBxNzhxcnBucjRhZThtdjVtdmJkZ3Jub0Bncm91cC5jYWxlbmRhci5nb29nbGUuY29t>`__
to stay informed. 

.. raw:: html

   <table class="colwidths-given longtable table" id="webinars-planned-table">
      <script type="text/javascript"charset="utf-8">
         d3.text("https://tomdrabas.com/data/webinars_planned.csv", function(data) {
               var parsedCSV = d3.csv.parseRows(data);

               var container = d3.select("#webinars-planned-table")
               var thead = container.append("thead");
               var tbody = container.append("tbody");

               var rows = tbody.selectAll("tr")
                  .data(parsedCSV).enter()
                  .append("tr")

               var cells = rows.selectAll("td")
                  .data(function(d) { return d; }).enter()
                  .append("td")
                  .text(function(d) {
                  if(!d.startsWith("https")) {
                     return d;
                  }
                  });

               cells.filter(function(d, i) { return d.startsWith("https")})
                  .append("a")
                  .attr("href", function(d) {
                     return d;
                  })
                  .html(function(d) {
                     return (d);
                  });
         });

      </script>
    </table>
   </div>

.. _webinars_delivered:

Webinars delivered
------------------

We have delievered multiple webinars with great content! Below is a list we released to date. 
Don't forget to `subscribe to our YouTube channel <https://www.youtube.com/channel/UCjxMdJuZ6d-c-X6xiWpN1eg>`__!

.. raw:: html

   <table class="colwidths-given longtable table" id="webinars-delivered-table">
      <script type="text/javascript"charset="utf-8">
         d3.text("https://tomdrabas.com/data/webinars_delivered.csv", function(data) {
               var parsedCSV = d3.csv.parseRows(data);

               var container = d3.select("#webinars-delivered-table")
               var thead = container.append("thead");
               var tbody = container.append("tbody");

               var rows = tbody.selectAll("tr")
                  .data(parsedCSV).enter()
                  .append("tr")

               var cells = rows.selectAll("td")
                  .data(function(d) { return d; }).enter()
                  .append("td")
                  .text(function(d) {
                  if(!d.startsWith("https")) {
                     return d;
                  }
                  });

               cells.filter(function(d, i) { return d.startsWith("https")})
                  .append("a")
                  .attr("href", function(d) {
                     return d;
                  })
                  .html(function(d) {
                     return (d);
                  });
         });

      </script>
    </table>
   </div>


.. toctree::
    :maxdepth: 2