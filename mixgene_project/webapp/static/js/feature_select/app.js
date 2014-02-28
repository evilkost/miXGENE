'use strict';

var PhenotypeEditor = angular.module("PhenotypeEditor",
    ['ngCookies', 'ngGrid', 'angularSpinner']
//    ,function ($interpolateProvider) {
//        $interpolateProvider.startSymbol("{$");
//        $interpolateProvider.endSymbol("$}");
//    }
);

PhenotypeEditor.run(function ($http, $cookies) {
    $http.defaults.headers.common['X-CSRFToken'] = $cookies['csrftoken'];
});

PhenotypeEditor.factory("phenoIO", function($http){
    var io = {
        pheno: {},
        exp_id: undefined,
        block_uuid: undefined
    };

    io.init_source = function(exp_id, block_uuid){
        io.exp_id = exp_id;
        io.block_uuid = block_uuid;
    };

    io.fetch_data = function(cb){
        if( io.exp_id === undefined || io.block_uuid === undefined){
            alert("Fatal error, no identifiers to fetch data from server");

        };
        $http({
            method: "GET",
            url: "/experiments/" + io.exp_id + "/blocks/" + io.block_uuid + "/phenotype_for_js"
        }).success(function(data, status, headers, config){
            io.pheno = data;
            document._io = io;
            cb()
        })
    }

    io.send_classes = function(method_name, to_send, on_success){
        console.log("SENDING: " + to_send);
        $http({
            method: "POST",
            url: "/experiments/" + io.exp_id+ "/blocks/"+
                io.block_uuid +"/actions/" + method_name,
            data: angular.toJson(to_send)
        }).success(function(){
            on_success();
        })
    }

    return io;
});

PhenotypeEditor.controller('PhenoCtrl', function($scope, phenoIO){
    $scope.init_done = false;
    $scope.phenoIO = phenoIO;
    $scope.features = [];


    $scope.gridOptions = {
        data: 'phenoIO.pheno.table',
        columnDefs: 'phenoIO.pheno.headers',
        selectedItems: $scope.selected_rows,
        enableColumnResize: true
    };


    $scope.on_data_fetched = function(){
        _.each($scope.phenoIO.pheno.headers, function(header){
            $scope.features.push({
                name: header.field,
                active: _.contains($scope.phenoIO.pheno.features, header.field)
            })

        });
        $scope.init_done = true;
        console.log("Initiated phenoIO with exp_id=" + $scope.phenoIO.exp_id +
            " block_uuid=" + $scope.phenoIO.block_uuid);
    };

    $scope.init = function(exp_id, block_uuid){
        console.log("Initiating phenoIO with exp_id=" + exp_id + " block_uuid=" + block_uuid);
        $scope.phenoIO.init_source(exp_id, block_uuid);
        $scope.phenoIO.fetch_data($scope.on_data_fetched);


    };


    $scope.switch_selection = function(feature){
        feature.active = !feature.active;
    };

    $scope.select_all = function(val){
        _.each($scope.features, function(feature){
            feature.active = val;
        });
    }

    $scope.save_selection = function(){
        var feature_list = [];
        _.each($scope.features, function(feature){
            if(feature.active == true){
                feature_list.push(feature.name);
            }
        });
        var to_send = {
            features: feature_list
        };
        $scope.phenoIO.send_classes("update_feature_selection", to_send, function(){
            toastr.info("Feature selection was stored ")
        });
    }



});