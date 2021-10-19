// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

package ihtml

//go:generate make-static-content/make-static-content ihtml stylesDotCSS              text/css                      s static-content/styles.css                                         styles_dot_css_.go

//go:generate make-static-content/make-static-content ihtml utilsDotJS                application/javascript        s static-content/utils.js                                           utils_dot_js_.go

//go:generate make-static-content/make-static-content ihtml jsontreeDotJS             application/javascript        s static-content/jsontree.js                                        jsontree_dot_js_.go

//go:generate make-static-content/make-static-content ihtml bootstrapDotCSS           text/css                      s static-content/bootstrap.min.css                                  bootstrap_dot_min_dot_css_.go

//go:generate make-static-content/make-static-content ihtml bootstrapDotJS            application/javascript        s static-content/bootstrap.min.js                                   bootstrap_dot_min_dot_js_.go
//go:generate make-static-content/make-static-content ihtml jqueryDotJS               application/javascript        s static-content/jquery.min.js                                      jquery_dot_min_dot_js_.go
//go:generate make-static-content/make-static-content ihtml popperDotJS               application/javascript        b static-content/popper.min.js                                      popper_dot_min_dot_js_.go

//go:generate make-static-content/make-static-content ihtml openIconicBootstrapDotCSS text/css                      s static-content/open-iconic/font/css/open-iconic-bootstrap.min.css open_iconic_bootstrap_dot_css_.go

//go:generate make-static-content/make-static-content ihtml openIconicDotEOT          application/vnd.ms-fontobject b static-content/open-iconic/font/fonts/open-iconic.eot             open_iconic_dot_eot_.go
//go:generate make-static-content/make-static-content ihtml openIconicDotOTF          application/font-sfnt         b static-content/open-iconic/font/fonts/open-iconic.otf             open_iconic_dot_otf_.go
//go:generate make-static-content/make-static-content ihtml openIconicDotSVG          image/svg+xml                 s static-content/open-iconic/font/fonts/open-iconic.svg             open_iconic_dot_svg_.go
//go:generate make-static-content/make-static-content ihtml openIconicDotTTF          application/font-sfnt         b static-content/open-iconic/font/fonts/open-iconic.ttf             open_iconic_dot_ttf_.go
//go:generate make-static-content/make-static-content ihtml openIconicDotWOFF         application/font-woff         b static-content/open-iconic/font/fonts/open-iconic.woff            open_iconic_dot_woff_.go
