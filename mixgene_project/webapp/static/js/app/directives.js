Constructor.directive("pallet", function () {
    return {
        restrict: 'AE',
        templateUrl: "/static/js/app/pallet.html"
    }
})

Constructor.directive("blockDir", function () {
    return {
        restrict: 'AE',
        replace: true,
        scope: {
            block: "="
        },
        templateUrl: "/static/js/app/block.html"
    }
})

Constructor.directive("sblockDir", function () {
    return {
        restrict: 'AE',
        replace: true,
//        scope: {
//            block:"="
//        },
        templateUrl: "/static/js/app/sblock.html"
    }
})

Constructor.directive("subblockcont", function () {
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
        controller: function ($scope) {
            document.tmp4 = $scope;

        }
    }
})

Constructor.directive("portsGroup", function () {
    return {
        restrict: 'A',
        replace: true,
        scope: {
            "groupName": '@',
            "block": '='
        },
        templateUrl: "/static/js/app/port_group.html",
        controller: function ($scope, blockAccess) {

            $scope.access = blockAccess;
        }

    }
})

Constructor.directive('capitalize', function () {
    // from http://stackoverflow.com/questions/16388562/angularjs-force-uppercase-in-textbox
    return {
        require: 'ngModel',
        link: function (scope, element, attrs, modelCtrl) {
            var capitalize = function (inputValue) {
                if( inputValue == null || inputValue == undefined){
                    return inputValue;
                }
                var capitalized = inputValue.toUpperCase();
                if (capitalized !== inputValue) {
                    modelCtrl.$setViewValue(capitalized);
                    modelCtrl.$render();
                }
                return capitalized;
            }
            modelCtrl.$parsers.push(capitalize);
            capitalize(scope[attrs.ngModel]);  // capitalize initial value
        }
    };
});