Constructor.factory("blockAccess", function($http){
    var access = {
        exp_id: document.exp.exp_id
    }

    access.blocks_by_bscope = {};
    access.block_bodies = {};
    access.vars_by_bscope = {};

    access.blocks_by_group_json = {};

    access.fetch_blocks = function(){
        $http({
            method: 'GET',
            url: '/experiments/' + access.exp_id + '/blocks/'
        }).success(function(data, status, headers, config){
            access.block_bodies = data.block_bodies;
            access.blocks_by_bscope = data.blocks_by_bscope;
            access.vars_by_bscope = data.vars_by_bscope;

            access.blocks_by_group = data.blocks_by_group;
            document.access = access;
        })
    }

    access.add_block = function(bscope, block_name){
        console.log("adding to "+ bscope + " block " + block_name);
        var request_body = angular.toJson({
            "block_name": block_name,
            "scope": bscope
        });
        console.log(request_body)
        $http({
            method: 'POST',
            url: '/experiments/' + access.exp_id + '/blocks/',
            data: request_body
        }).success(function(data, status, headers, config){
            access.block_bodies = data.block_bodies;
            access.blocks_by_bscope = data.blocks_by_bscope;
            access.vars_by_bscope = data.vars_by_bscope;
            document.access = access;
        })
    }

    access.reload_block = function(block){
        block.is_block_updating = true;
        $http({
            method: 'GET',
            url: '/experiments/' + access.exp_id + '/blocks/' + block.uuid
            ,body: angular.toJson(block)
        }).success(function(data, status, headers, config){
            access.block_bodies[data.uuid] = data;
        })
    }

    access.send_action = function(block, action_code){
        block.is_block_updating = true;
        $http({
            method: 'POST',
            url: '/experiments/' + access.exp_id +
                '/blocks/' + block.uuid + "/actions/" + action_code,
            data:  angular.toJson(block)
        }).success(function(data, status, headers, config){
            access.block_bodies[data.uuid] = data;
        })
    }

    access.get_port_input_options = function (port){
        // TODO: split into another service with dependency to blockAccess
        var result = []
        angular.forEach(port.bscopes, function(bscope, idx){
            angular.forEach(access.vars_by_bscope[bscope][port.data_type], function(option, idx){
                result.push(option);
            })
        })
        return result;
    }

    access.fetch_blocks();
    return access;
});
