'use strict';

var Constructor = angular.module("Constructor", [
        'ngCookies',
        'ngTable',
        'ngSanitize',
        'angularFileUpload',
        'highcharts-ng',
        'ui.bootstrap'
    ],
    function ($interpolateProvider) {
        $interpolateProvider.startSymbol("{$");
        $interpolateProvider.endSymbol("$}");
    }
);

Constructor.run(function ($http, $cookies) {
    $http.defaults.headers.common['X-CSRFToken'] = $cookies['csrftoken'];
})



