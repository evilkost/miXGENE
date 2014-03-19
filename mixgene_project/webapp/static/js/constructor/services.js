Constructor.factory("blockAccess", function($http, $log){
    var access = {}


    access.blocks_by_bscope = {};
    access.block_bodies = {};
    access.scopes = {};

    access.vars = {};
    access.vars_by_key = {};

    access.blocks_by_group_json = {};

    // TODO: fetch from server
    access.available_data_types = [
        "ExpressionSet",
        "GeneSets",
        "BinaryInteraction"
    ]

    var sockjs = new SockJS('/subscribe');
    sockjs.onopen = function(){
        console.log('[*] open sockjs, protocol: ' + sockjs.protocol);

        // TODO: export public key in exp object from server
        sockjs.send(angular.toJson({type: "init", content: "ENPK-" + access.exp_id}));
    };
    toastr.options["timeOut"] = 100000;
    toastr.options["extendedTimeOut"] = 0;
    toastr.options["closeButton"] = true;

    sockjs.onmessage = function(e){
        if(e.type === "message"){
            var msg = angular.fromJson(e.data);
            console.log("Got message through WS: " + e.data);
            if( msg.type === "updated_block"){
                var block_ = access.block_bodies[msg.block_uuid];
                access.reload_block(block_);
            }

            if( msg.type === "updated_all"){
                access.fetch_blocks();
                // TODO: reload scope variables when added
            }

            if( ! msg.silent){
                toastr[msg.mode](msg.comment);
            }

        }

    }
    access.sockjs = sockjs;

    access.exp_sub_resource = function(sub_resource, on_success){
        $http({
            method: 'GET',
            url: '/experiments/' + access.exp_id + '/sub/' + sub_resource
        }).success(function(data){
            on_success(data);
        });
    }


    access.fetch_blocks = function(){
        $http({
            method: 'GET',
            url: '/experiments/' + access.exp_id + '/blocks/'
        }).success(function(data, status, headers, config){
            access.block_bodies = data.block_bodies;
            access.blocks_by_bscope = data.blocks_by_bscope;

            access.blocks_by_group = data.blocks_by_group;
            access.scopes = data.scopes;

            access.vars = data.vars;
            access.vars_by_key = data.vars_by_key;

            document.access = access;
        })
    }

    access.add_block = function(bscope, block_name){
        console.log("adding to "+ bscope + " block " + block_name);
        var request_body = angular.toJson({
            "block_name": block_name,
            "scope_name": bscope
        });
        console.log(request_body)
        $http({
            method: 'POST',
            url: '/experiments/' + access.exp_id + '/blocks/',
            data: request_body
        }).success(function(data, status, headers, config){
            access.block_bodies = data.block_bodies;
            access.blocks_by_bscope = data.blocks_by_bscope;
            access.scopes = data.scopes;

            access.vars = data.vars;
            access.vars_by_key = data.vars_by_key;

            document.access = access;
        })
    }

    access.reload_block = function(block){
//        $log.debug(block)
        block.is_block_updating = true;
        $http({
            method: 'GET',
            url: '/experiments/' + access.exp_id + '/blocks/' + block.uuid
            ,body: angular.toJson(block)
        }).success(function(data, status, headers, config){
            access.block_bodies[data.uuid] = data;
            $log.debug(data);
        })
    }

    access.send_action = function(block, action_code, do_reload_all, on_success){
        if(typeof(do_reload_all)==='undefined'){ do_reload_all = false };


        console.log(block);
        block.is_block_updating = true;
        $http({
            method: 'POST',
            url: '/experiments/' + access.exp_id +
                '/blocks/' + block.uuid + "/actions/" + action_code,
            data:  angular.toJson(block)
        }).success(function(data, status, headers, config){
            if(do_reload_all){
                access.fetch_blocks();
            } else {
                // TODO: add a special field in block to indicate
                //          that we need to reload all blocks
                access.block_bodies[data.uuid] = data;

            }
            if(typeof(on_success) != "undefined"){
                on_success();
            }
        })
    }

    access.block_method = function(block, action_code, on_success){
        $http({
            method: 'POST',
            url: '/experiments/' + access.exp_id +
                '/blocks/' + block.uuid + "/actions/" + action_code,
            data:  angular.toJson(block)
        }).success(function(data, status, headers, config){
            document._recv = data;
            if(typeof(on_success) != 'undefined'){
                on_success(data);
            }
        })
    }

    access.fnFilterVarsByType = function(data_type_list){
        return function(scope_var){
            return _.contains(data_type_list, scope_var.data_type);
        }
    }
    access.fnFilterVarsByScope = function(scopes_list){
        return function(scope_var){
            return _.contains(scopes_list, scope_var.scope_name);
        }
    }
    access.fnFilterVarsByBlockUUID = function(uuid_list){
        return function(scope_var){
            return !_.contains(uuid_list, scope_var.block_uuid);
        }
    }
    access.fnIncludeVarsByBlockUUID = function(uuid_list){
        return function(scope_var){
            return _.contains(uuid_list, scope_var.block_uuid);
        }
    }

    access.init = function(exp_id, mode){
        access.mode = mode;
        access.exp_id = exp_id;
        access.exp = {
            exp_id: exp_id
        }
        access.fetch_blocks();

    }
    return access;
});

Constructor.factory('RecursionHelper', ['$compile', function($compile){
    // http://stackoverflow.com/a/18609594
    var RecursionHelper = {
        compile: function(element){
            var contents = element.contents().remove();
            var compiledContents;
            return function(scope, element){
                if(!compiledContents){
                    compiledContents = $compile(contents);
                }
                compiledContents(scope, function(clone){
                    element.append(clone);
                });
            };
        }
    };

    return RecursionHelper;
}]);
