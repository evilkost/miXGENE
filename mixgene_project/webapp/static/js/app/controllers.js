Constructor.controller('MainCtrl', function($scope){
    $scope.show_old_worktable = false;

    $scope.toggle_show_old_worktable = function(){
        $scope.show_old_worktable = !$scope.show_old_worktable;
    }
})

Constructor.controller('WorktableCtrl', function WorktableCtrl($scope, blockAccess){

    $scope.exp_id = document.exp.exp_id;

    $scope.access = blockAccess;

    $scope.available_blocks = document.blocks_by_group; // TODO: move to service
    $scope.blocksOrder = document.blocks_order;
})

Constructor.controller('BlockCtrl', function BlockCtrl($scope){
    $scope.has_errors = $scope.block.errors.length > 0;

    $scope.show_panel_body = true;
    $scope.toggleBodyVisibility = function(){
        $scope.show_panel_body = !$scope.show_panel_body;
    };

})

Constructor.controller('PortGroupCtrl', function PortGroupCtrl($scope){
    //$scope.showPortGroup = $scope.ports != null;
    $scope.showPortGroup = true;

})

Constructor.controller('PortCtrl', function PortCtrl($scope){
    if( $scope.port.bound_key == undefined || $scope.port.bound_key == null){
        if( $scope.port.options.length > 0){
            $scope.port.bound_key = $scope.port.options[0].key;
        }
    }

    $scope.render_option = function( option ){
        if( option != undefined && option != null){
            return option.block_alias + " -> " + option.var_name;
        } else {
            return "--error--"
        }
    }

})

Constructor.controller('BlockActionCtrl', function($scope, $http, $cookies){

})