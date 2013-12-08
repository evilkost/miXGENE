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

Constructor.directive("block", function(){
    return {
        restrict: 'E',
        templateUrl: "/static/js/app/block.html"
        //template: "<div>{$ block.name $}</div>"
    }
})

Constructor.directive("portsGroup", function(){
    return {
        restrict: 'A',
        replace: true,
        scope: {
            block: '=',
            groupName: '@'
        },
        templateUrl: "/static/js/app/port_group.html"
        //template: "<div>XXX</div>"
    }
})

Constructor.controller('MainCtrl', function($scope){
    $scope.show_old_worktable = false;

    $scope.toggle_show_old_worktable = function(){
        $scope.show_old_worktable = !$scope.show_old_worktable;
    }
})

Constructor.controller('WorktableCtrl', function WorktableCtrl($scope){

    $scope.available_blocks = document.blocks_by_group;
    $scope.blocks = document.blocks_jsonified;
    $scope.exp = document.exp;
})

Constructor.controller('BlockCtrl', function BlockCtrl($scope, $http){
    $scope.has_errors = $scope.block.errors.length > 0;

    $scope.show_panel_body = true;
    $scope.toggleBodyVisibility = function(){
        $scope.show_panel_body = !$scope.show_panel_body;
    };

    $scope.reloadBlock = function(){
        //$scope.show_panel_body = false;
        $scope.is_block_updating = true;
        // TODO: create service
        $http({
            method: 'GET',
            url: '/experiments/' + $scope.exp.exp_id +
                '/blocks/' + $scope.block.uuid,
            data:  JSON.stringify($scope.block)
        }).success(function(data, status, headers, config){
            $scope.blocks[data.uuid] = data;
            $scope.is_block_updating = false;
        })


    };

    $scope.send_action = function(action_code){
        $scope.is_block_updating = true;
        $http({
            method: 'POST',
            url: '/experiments/' + $scope.exp.exp_id +
                '/blocks/' + $scope.block.uuid + "/actions/" + action_code,
            data:  JSON.stringify($scope.block)
        }).success(function(data, status, headers, config){
                $scope.blocks[data.uuid] = data;
                $scope.is_block_updating = false;
            })
    }
})

Constructor.controller('PortGroupCtrl', function PortGroupCtrl($scope){
//    $scope.ports = function(){
//        return $scope.block.ports[$scope.groupName];
//    }

    //$scope.showPortGroup = $scope.ports.length > 0;
    $scope.showPortGroup = true;


    //alert($scope)
})
Constructor.controller('PortCtrl', function PortCtrl($scope, $log){
    if( $scope.port.bound_key == undefined || $scope.port.bound_key == null){
        if( $scope.port.options.length > 0){
            $scope.port.bound_key = $scope.port.options[0].key;
        }
    }
    //$log.info(JSON.stringify($scope.port.options));
    $scope.render_option = function( option ){
        if( option != undefined && option != null){
            return option.block_alias + " -> " + option.var_name;
        } else {
            return "--error--"
        }
    }

})

Constructor.controller('BlockActionCtrl', function($scope, $http, $cookies){
    document.tmp2 = $scope;


})


//Constructor.filter("select_input_group", function(){
//    return function(input, groupName){
//        //alert(JSON.stringify(input));
//        //alert(JSON.stringify(groupName));
//    }
//})