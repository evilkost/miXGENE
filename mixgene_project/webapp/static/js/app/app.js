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
        scope: {
            block:"="
        },
        templateUrl: "/static/js/app/block.html"
    }
})

Constructor.directive("sblockDir", function(){
    return {
        restrict: 'AE',
        replace: true,
//        scope: {
//            block:"="
//        },
        templateUrl: "/static/js/app/sblock.html"
    }
})

Constructor.directive("subblockcont", function(){
    // http://jsfiddle.net/brendanowen/uXbn6/8/
    //
    return {
        restrict: 'AE',
        scope: {
            block: '='
        },
        template: "<div>" +
            "   <div ng-repeat='sb in block.sub_blocks'> {$ sb $} " +
            "       <div block-dir block='sb'></div>" +
            "</div>" +
            "</div>",
        controller: function($scope){
            document.tmp4 = $scope;

        }
    }
})

Constructor.directive("portsGroup", function(){
    return {
        restrict: 'A',
        replace: true,
        scope: {
            "groupName": '@',
            "block": '='
        },
        templateUrl: "/static/js/app/port_group.html",
        controller: function($scope, blockAccess){

            $scope.access = blockAccess;
        }

    }
})
