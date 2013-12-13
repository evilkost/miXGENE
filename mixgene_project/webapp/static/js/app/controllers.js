Constructor.controller('MainCtrl', function($scope, blockAccess){
    $scope.show_old_worktable = false;
    $scope.access = blockAccess;
    $scope.exp_id = $scope.access.exp_id;

    $scope.toggle_show_old_worktable = function(){
        $scope.show_old_worktable = !$scope.show_old_worktable;
    }
})

Constructor.controller('WorktableCtrl', function WorktableCtrl($scope, blockAccess){
    $scope.access = blockAccess;

    $scope.available_blocks = document.blocks_by_group; // TODO: move to service
    $scope.blocksOrder = document.blocks_order;
})

Constructor.controller('BlockCtrl', function BlockCtrl($scope, blockAccess){
    $scope.access = blockAccess;
    $scope.exp_id = $scope.access.exp_id;
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

Constructor.controller('PortCtrl', function PortCtrl($scope, blockAccess){
    $scope.access = blockAccess;
    $scope.get_option_key = function( option ){
        return option.block_uuid + "_" + option.var_name;
    }
    $scope.get_option_title = function( option ){
        if( option != undefined && option != null){
            return option.block_alias + " -> " + option.var_name;
        } else {
            return "--error--"
        }
    }

    $scope.options = blockAccess.get_port_input_options($scope.port);

    if( $scope.port.bound_key == undefined || $scope.port.bound_key == null){
        if( $scope.options.length > 0){
            $scope.port.bound_key = $scope.get_option_key($scope.options[0]);
        }
    }



})

Constructor.controller('ddCtrl', function($scope, blockAccess){
//    $scope.block = $scope.access.block_dict[$scope.uuid];
    $scope.access = blockAccess;

    //$scope.block = $scope.access.blocks[$scope.data.num];
})


Constructor.controller('ttCtrl', function($scope, blockAccess){
//    alert($scope.uuid);

    $scope.access = blockAccess;
    $scope.access.load_block($scope.uuid);
//    document.access = blockAccess;
//    $scope.block = $scope.access.block_dict[$scope.uuid];

})



Constructor.controller('formFieldCtrl', function($scope, $http, $cookies){
    $scope.template = "/static/js/app/forms/field_" +
        $scope.$parent.param_proto.input_type + ".html";
})

Constructor.controller('BlockActionCtrl', function($scope, $http, $cookies){

})

