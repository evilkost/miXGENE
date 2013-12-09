'use strict';

var Constructor = angular.module("Constructor", ['ngCookies'],
    function ($interpolateProvider) {
        $interpolateProvider.startSymbol("{$");
        $interpolateProvider.endSymbol("$}");
    }
);

Constructor.run(function ($http, $cookies) {
    $http.defaults.headers.common['X-CSRFToken'] = $cookies['csrftoken'];
})

Constructor.directive("pallet", function(){
    return {
        restrict: 'E',
        templateUrl: "/static/js/app/pallet.html"
    }
})

Constructor.directive("blockDir", function(){
    return {
        restrict: 'AE',
        replace: true,
        templateUrl: "/static/js/app/block.html"
    }
})

Constructor.directive("portsGroup", function(){
    return {
        restrict: 'A',
        replace: true,
        templateUrl: "/static/js/app/port_group.html"
    }
})
