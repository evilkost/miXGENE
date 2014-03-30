Constructor.directive("pallet", function () {
    return {
        restrict: 'AE',
        scope: {
            scopeName: "=",
            blockName: "="
        },
        templateUrl: "/static/js/constructor/partials/pallet.html"
    }
})

Constructor.directive("blockDir", function (RecursionHelper) {
    return {
        restrict: 'AE',
        replace: true,
        scope: {
            block: "="
        },
        templateUrl: "/static/js/constructor/partials/block.html",
        compile: function(element) {
            return RecursionHelper.compile(element);
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
        templateUrl: "/static/js/constructor/partials/port_group.html",
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