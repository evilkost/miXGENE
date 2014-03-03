'use strict';

var Constructor = angular.module("Constructor",
        ['ngCookies', 'angularFileUpload', 'highcharts-ng'],
    function ($interpolateProvider) {
        $interpolateProvider.startSymbol("{$");
        $interpolateProvider.endSymbol("$}");
    }
);

Constructor.run(function ($http, $cookies) {
    $http.defaults.headers.common['X-CSRFToken'] = $cookies['csrftoken'];
})



