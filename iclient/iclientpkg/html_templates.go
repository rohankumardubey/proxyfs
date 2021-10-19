// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

package iclientpkg

// To use: fmt.Sprintf(indexDotHTMLTemplate, proxyfsVersion)
//                                               %[1]v
const indexDotHTMLTemplate string = `<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1, shrink-to-fit=no">
    <link rel="stylesheet" href="/bootstrap.min.css">
    <link rel="stylesheet" href="/styles.css">
    <title>iclient</title>
  </head>
  <body>
    <nav class="navbar navbar-expand-lg navbar-dark bg-dark fixed-top">
      <button class="navbar-toggler" type="button" data-toggle="collapse" data-target="#navbarNavDropdown" aria-controls="navbarNavDropdown" aria-expanded="false" aria-label="Toggle navigation">
        <span class="navbar-toggler-icon"></span>
      </button>
      <div class="collapse navbar-collapse" id="navbarNavDropdown">
        <ul class="navbar-nav mr-auto">
          <li class="nav-item active">
            <a class="nav-link" href="/">Home <span class="sr-only">(current)</span></a>
          </li>
          <li class="nav-item">
            <a class="nav-link" href="/config">Config</a>
          </li>
          <li class="nav-item">
            <a class="nav-link" href="/stats">Stats</a>
          </li>
          <li class="nav-item">
            <a class="nav-link" href="/leases">Leases</a>
          </li>
        </ul>
        <span class="navbar-text">Version %[1]v</span>
      </div>
    </nav>
    <div class="container">
      <nav aria-label="breadcrumb">
        <ol class="breadcrumb">
          <li class="breadcrumb-item active" aria-current="page">Home</li>
        </ol>
      </nav>
      <h1 class="display-4">
        ProxyFS iclient
      </h1>
      <div class="card-deck">
        <div class="card mb-4">
          <div class="card-body">
            <h5 class="card-title">Configuration parameters</h5>
            <p class="card-text">Diplays a JSON representation of the active configuration.</p>
          </div>
          <ul class="list-group list-group-flush">
            <li class="list-group-item">
              <a href="/config" class="card-link">Configuration Parameters</a>
          </ul>
        </div>
        <div class="w-100 d-none d-sm-block d-md-none"><!-- wrap every 1 on sm--></div>
        <div class="card mb-4">
          <div class="card-body">
            <h5 class="card-title">Stats</h5>
            <p class="card-text">Displays current statistics.</p>
          </div>
          <ul class="list-group list-group-flush">
            <li class="list-group-item">
              <a href="/stats" class="card-link">Stats Page</a>
          </ul>
        </div>
        <div class="w-100 d-none d-sm-block d-md-none"><!-- wrap every 1 on sm--></div>
        <div class="w-100 d-none d-md-block d-lg-none"><!-- wrap every 2 on md--></div>
        <div class="w-100 d-none d-lg-block d-xl-none"><!-- wrap every 2 on lg--></div>
        <div class="w-100 d-none d-xl-block"><!-- wrap every 3 on xl--></div>
        <div class="w-100 d-none d-sm-block d-md-none"><!-- wrap every 1 on sm--></div>
        <div class="card mb-4">
          <div class="card-body">
            <h5 class="card-title">Leases</h5>
            <p class="card-text">Examine leases currently active on this iclient instance.</p>
          </div>
          <ul class="list-group list-group-flush">
            <li class="list-group-item">
              <a href="/leases" class="card-link">Leases Page</a>
          </ul>
        </div>
      </div>
    </div>
    <script src="/jquery.min.js"></script>
    <script src="/popper.min.js"></script>
    <script src="/bootstrap.min.js"></script>
  </body>
</html>
`

// To use: fmt.Sprintf(configTemplate, proxyfsVersion, confMapJSONString)
//                                          %[1]v            %[2]v
const configTemplate string = `<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1, shrink-to-fit=no">
    <link rel="stylesheet" href="/bootstrap.min.css">
    <link rel="stylesheet" href="/styles.css">
    <title>Config</title>
  </head>
  <body>
    <nav class="navbar navbar-expand-lg navbar-dark bg-dark fixed-top">
      <button class="navbar-toggler" type="button" data-toggle="collapse" data-target="#navbarNavDropdown" aria-controls="navbarNavDropdown" aria-expanded="false" aria-label="Toggle navigation">
        <span class="navbar-toggler-icon"></span>
      </button>
      <div class="collapse navbar-collapse" id="navbarNavDropdown">
        <ul class="navbar-nav mr-auto">
          <li class="nav-item">
            <a class="nav-link" href="/">Home</a>
          </li>
          <li class="nav-item active">
            <a class="nav-link" href="/config">Config <span class="sr-only">(current)</span></a>
          </li>
          <li class="nav-item">
            <a class="nav-link" href="/stats">Stats</a>
          </li>
          <li class="nav-item">
            <a class="nav-link" href="/leases">Leases</a>
          </li>
        </ul>
        <span class="navbar-text">Version %[1]v</span>
      </div>
    </nav>
    <div class="container">
      <nav aria-label="breadcrumb">
        <ol class="breadcrumb">
          <li class="breadcrumb-item"><a href="/">Home</a></li>
          <li class="breadcrumb-item active" aria-current="page">Config</li>
        </ol>
      </nav>
      <h1 class="display-4">
        Config
      </h1>
      <pre class="code" id="json_data"></pre>
    </div>
    <script src="/jquery.min.js"></script>
    <script src="/popper.min.js"></script>
    <script src="/bootstrap.min.js"></script>
    <script src="/jsontree.js"></script>
    <script type="text/javascript">
      var json_data = %[2]v;
      document.getElementById("json_data").innerHTML = JSONTree.create(json_data, null, 1);
      JSONTree.collapse();
    </script>
  </body>
</html>
`

// To use: fmt.Sprintf(leasesTemplate, proxyfsVersion, inodeLeaseTableJSONString)
//                                          %[1]v            %[2]v
const leasesTemplate string = `<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1, shrink-to-fit=no">
    <link rel="stylesheet" href="/bootstrap.min.css">
    <link rel="stylesheet" href="/styles.css">
    <link href="/open-iconic/font/css/open-iconic-bootstrap.min.css" rel="stylesheet">
    <title>Leases</title>
  </head>
  <body>
    <nav class="navbar navbar-expand-lg navbar-dark bg-dark fixed-top">
      <button class="navbar-toggler" type="button" data-toggle="collapse" data-target="#navbarNavDropdown" aria-controls="navbarNavDropdown" aria-expanded="false" aria-label="Toggle navigation">
        <span class="navbar-toggler-icon"></span>
      </button>
      <div class="collapse navbar-collapse" id="navbarNavDropdown">
        <ul class="navbar-nav mr-auto">
          <li class="nav-item">
            <a class="nav-link" href="/">Home</a>
          </li>
          <li class="nav-item">
            <a class="nav-link" href="/config">Config</a>
          </li>
          <li class="nav-item">
            <a class="nav-link" href="/stats">Stats</a>
          </li>
          <li class="nav-item active">
            <a class="nav-link" href="/leases">Leases <span class="sr-only">(current)</span></a>
          </li>
        </ul>
        <span class="navbar-text">Version %[1]v</span>
      </div>
    </nav>
    <div class="container">
      <nav aria-label="breadcrumb">
        <ol class="breadcrumb">
          <li class="breadcrumb-item"><a href="/">Home</a></li>
          <li class="breadcrumb-item active" aria-current="page">Leases</li>
        </ol>
      </nav>
      <h1 class="display-4">
        Leases
      </h1>
      <!-- Back to top button -->
      <button type="button" class="btn btn-primary btn-floating btn-lg" id="btn-back-to-top">
        <span class="oi oi-chevron-top"></span>
      </button>
      <pre class="code" id="json_data"></pre>
    </div>
    <script src="/jquery.min.js"></script>
    <script src="/popper.min.js"></script>
    <script src="/bootstrap.min.js"></script>
    <script src="/utils.js"></script>
    <script type="text/javascript">
      var json_data = %[2]v;

      function doDemote() {
        console.log("in doDemote");
        fetch("/leases/demote", {
          method: 'POST'
        }).then(res => {
          location.reload();
        });
      }

      function doRelease() {
        console.log("in doRelease");
        fetch("/leases/release", {
          method: 'POST'
        }).then(res => {
          location.reload();
        });
      }

      const addMarkup = function(json_data) {
        let table_markup = "";

        table_markup += "      <table class=\"table table-sm table-striped table-hover\" id=\"leases-table\">";
        table_markup += "        <tbody>";
        table_markup += "          <tr>";
        table_markup += "            <td class=\"row\"><a href=\"#\" onclick=\"doDemote()\" class=\"btn btn-sm btn-primary\">Demote All</a></td>\n";
        table_markup += "            <td class=\"text-right\"><a href=\"#\" onclick=\"doRelease()\" class=\"btn btn-sm btn-primary\">Release All</a></td>\n";
        table_markup += "          </tr>";
        table_markup += "        </tbody>";
        table_markup += "      </table>";

        table_markup += "      <table class=\"table table-sm table-striped table-hover\" id=\"leases-table\">";
        table_markup += "        <thead>";
        table_markup += "          <tr>";
        table_markup += "            <th scope=\"row\">InodeNumber</th>";
        table_markup += "            <th class=\"text-right\">LeaseState</th>";
        table_markup += "          </tr>";
        table_markup += "        </thead>";
        table_markup += "        <tbody>";

        for (const lease of json_data) {
          table_markup += "          <tr>";
          table_markup += "            <td scope=\"row\">" + lease.InodeNumber + "</td>";
          table_markup += "            <td class=\"text-right\">" + lease.State + "</td>";
          table_markup += "          </tr>";
        }

        table_markup += "        </tbody>";
        table_markup += "      </table>";

        return table_markup;
      }

      document.getElementById("json_data").innerHTML = addMarkup(json_data);

      // Fancy back to top behavior
      addBackToTopBehavior();
    </script>
  </body>
</html>
`
