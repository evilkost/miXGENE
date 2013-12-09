Constructor.factory("blockAccess", function($http){
    var access = {
        exp_id: document.exp.exp_id
    }
    access.fetch_blocks = function(){
        $http({
            method: 'GET',
            url: '/experiments/' + access.exp_id + '/blocks/'
        }).success(function(data, status, headers, config){
            access.blocks = data.blocks;

            access.uuid_to_idx = {}
            angular.forEach(access.blocks, function(block, idx){
                this[block.uuid] = idx
            },
                access.uuid_to_idx
            );

        })
    }

    access.reload_block = function(block){
        block.is_block_updating = true;
        $http({
            method: 'GET',
            url: '/experiments/' + access.exp_id + '/blocks/' + block.uuid,
            body: angular.toJson(block)
        }).success(function(data, status, headers, config){
            var block_idx = access.uuid_to_idx[block.uuid];
            access.blocks[block_idx] = data;
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
                var block_idx = access.uuid_to_idx[block.uuid];
                access.blocks[block_idx] = data;
        })
    }

    access.fetch_blocks();
    return access;
});
