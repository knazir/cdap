angular.module(PKG.name + '.feature.flows')
  .controller('FlowsDetailController', function($scope, MyDataSource, $state, FlowDiagramData, myHelpers) {
    var dataSrc = new MyDataSource($scope),
        basePath = '/apps/' + $state.params.appId + '/flows/' + $state.params.programId;

    var metricFlowletPath = '/metrics/query?metric=system.process.events.processed' +
                          '&context=ns.' +
                          $state.params.namespace +
                          '.app.' + $state.params.appId +
                          '.flow.' + $state.params.programId +
                          '.flowlet.',
        metricStreamPath = '/metrics/query?metric=system.collect.events' +
                           '&context=namespace.' +
                           $state.params.namespace +
                           '.stream.';

    FlowDiagramData.fetchData($state.params.appId, $state.params.programId)
      .then(function(data) {
        $scope.data = data;
        pollMetrics();
      });

    function pollMetrics() {
      var nodes = $scope.data.nodes;
      // Requesting Metrics data
      angular.forEach(nodes, function (node) {
        console.info('Polling on Metrics');
        dataSrc.poll({
          _cdapPath: (node.type === 'STREAM' ? metricStreamPath: metricFlowletPath) + node.name + '&aggregate=true',
          method: 'POST'
        }, function (data) {
            $scope.data.metrics[node.name] = myHelpers.objectQuery(data, 'series' , 0, 'data', 0, 'value') || 0;
          });
      });
    }

    console.info('Polling on active Runs');
    dataSrc.poll({
      _cdapNsPath: basePath + '/runs?status=running'
    }, function(res) {
        $scope.activeRuns = res.length;
      });

    $scope.do = function(action) {
      dataSrc.request({
        _cdapNsPath: basePath + '/' + action,
        method: 'POST'
      });
    };
  });
