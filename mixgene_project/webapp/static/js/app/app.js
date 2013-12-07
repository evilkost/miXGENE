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


function WorktableCtrl($scope){
    $scope.data = {msg: "panel"};
    $scope.available_blocks = document.blocks_by_group;
    $scope.blocks = document.blocks_jsonified;
}

Constructor.directive("pallet", function(){
    return {
        restrict: 'E',
        templateUrl: "/static/js/app/pallet.html"
    }
})

Constructor.directive("block", function(){
    return {
        restrict: 'E',
        templateUrl: "/static/js/app/block.html"
        //template: "<div>{$ block.name $}</div>"
    }
})

function BlockCtrl($scope){
    $scope.has_errors = $scope.block.errors.length > 0;

    $scope.show_panel_body = true;
    $scope.toggleBodyVisibility = function(){
        $scope.show_panel_body = !$scope.show_panel_body;
    };

}
