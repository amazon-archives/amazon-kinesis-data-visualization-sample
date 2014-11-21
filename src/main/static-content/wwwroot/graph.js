/*
 * Copyright 2014 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Amazon Software License (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://aws.amazon.com/asl/
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

// When the page loads create our graph and start updating it.
$(function() {
  graph.inject();
  uiHelper.decorate();
  uiHelper.start();
});

/**
 * Represents a Flot time series graph that is capable of updating itself with
 * new data.
 */
var Graph = function() {

  var graph, totalDurationToGraphInSeconds = 120;

  return {
    /**
     * @returns {number} the total duration of time, in seconds, this graph will display.
     */
    getTotalDurationToGraphInSeconds : function() {
      return totalDurationToGraphInSeconds;
    },

    /**
     * Creates the graph and injects in into the element with id="graph".
     */
    inject : function() {
      graph = $.plot("#graph", {},
          {
            // Define the colors and y-axis margins for the graph.
            grid : {
              borderWidth : 1,
              minBorderMargin : 20,
              labelMargin : 10,
              backgroundColor : {
                colors : [ "#fff", "#fff5e6" ]
              },
              margin : {
                top : 8,
                bottom : 20,
                left : 20
              },
            },
            // Do not render shadows for our series lines. This just slows us
            // down.
            series : {
              shadowSize : 0
            },
            // Set up the y-axis to initially show 0-10. This is dynamically
            // adjusted as data is updated.
            yaxis : {
              min : 0,
              max : 10
            },
            // The x-axis is time-based. The local browser's timezone will be
            // used to interpret timestamps. The range is dynamically adjusted
            // as data is updated.
            xaxis : {
              mode : "time",
              timezone : "browser",
              timeformat : "%M:%S",
              min : (new Date()).getTime()
                  - (totalDurationToGraphInSeconds * 1000),
              max : (new Date()).getTime()
            },
            // Show the legend of unique referrers in the upper-right corner of
            // the graph.
            legend : {
              show : true,
              position : "nw"
            }
          });

      // Create y-axis label and inject it into the graph container
      var yaxisLabel = $("<div class='axisLabel yaxisLabel'></div>").text(
          "Requests sent from referrer over 10 seconds").appendTo("#graph");
      // Center the y-axis along the left side of the graph
      yaxisLabel.css("margin-top", yaxisLabel.width() / 2 - 20);
    },

    /**
     * Update the graph to use the data provided. This completely replaces any
     * existing data and recomputes the axes' range.
     *
     * @param {Object}
     *          flotData Flot formatted data object that should include at a
     *          minimum series labels and their data in the format: { label: "my
     *          series", data: [[0, 10], [1, 100]] }
     */
    update : function(flotData) {
      graph.setData(flotData);

      // Calculate min and max value to update y-axis range.
      var getValue = function(tuple) {
        // Flot data values are stored as the second element of each data array
        return tuple[1];
      };
      var max = Number.MIN_VALUE;
      flotData.forEach(function(d) {
        m = Math.max.apply(Math, d.data.map(getValue));
        max = Math.max(m, max);
      });
      var min = Number.MAX_VALUE;
      flotData.forEach(function(d) {
        m = Math.min.apply(Math, d.data.map(getValue));
        min = Math.min(m, min);
      });

      // Adjust the y-axis for min/max of our new data
      graph.getOptions().yaxes[0].max = Math.min(max, max)
      graph.getOptions().yaxes[0].min = min

      // Adjust the x-axis to move in real time and show at most the total
      // duration to graph as configured above
      graph.getOptions().xaxes[0].min = (new Date()).getTime()
          - (totalDurationToGraphInSeconds * 1000),
          graph.getOptions().xaxes[0].max = (new Date()).getTime()

      // Redraw the graph data and axes
      graph.draw();
      graph.setupGrid();
    }
  }
}

/**
 * A collection of methods used to manipulate visible elements of the page.
 */
var UIHelper = function(data, graph) {
  // How frequently should we poll for new data and update the graph?
  var updateIntervalInMillis = 400;
  // How often should the top N display be updated?
  var intervalsPerTopNUpdate = 5;
  // How far back should we fetch data at every interval?
  var rangeOfDataToFetchEveryIntervalInSeconds = 2;
  // What should N be for our Top N display?
  var topNToCalculate = 3;
  // Keep track of when we last updated the top N display.
  var topNIntervalCounter = 1;
  // Controls the update loop.
  var running = true;
  // Set the active resource to query for counts when updating data.
  var activeResource = "/index.html";

  /**
   * Fetch counts from the last secondsAgo seconds.
   *
   * @param {string}
   *          resource The resource to fetch counts for.
   * @param {number}
   *          secondsAgo The range in seconds since now to fetch counts for.
   * @param {function}
   *          callback The callback to invoke when data has been updated.
   */
  var updateData = function(resource, secondsAgo, callback) {
    // Fetch data from our data provider
    provider.getData(resource, secondsAgo, function(newData) {
      // Store the data locally
      data.addNewData(newData);
      // Remove data that's outside the window of data we are displaying. This
      // is unnecessary to keep around.
      data.removeDataOlderThan((new Date()).getTime()
          - (graph.getTotalDurationToGraphInSeconds() * 1000));
      if (callback) {
        callback();
      }
    });
  }

  /**
   * Update the top N display.
   */
  var updateTopN = function() {
    var topN = data.getTopN(topNToCalculate);

    var table = $("<table/>").addClass("topN");
    $.each(topN, function(_, v) {
      console.loog
      var row = $("<tr/>");
      row.append($("<td/>").addClass('referrerColumn').text(v.referrer));
      row.append($("<td/>").addClass('countColumn').text(v.count));
      table.append(row);
    });

    $("#topN").html(table);
  }

  /**
   * Update the graph with new data.
   */
  var update = function() {
    // Update our local data for the active resource
    updateData(activeResource, rangeOfDataToFetchEveryIntervalInSeconds);

    // Update top N every intervalsPerTopNUpdate intervals
    if (topNIntervalCounter++ % intervalsPerTopNUpdate == 0) {
      updateTopN(data);
      topNIntervalCounter = 1;
    }

    // Update the graph with our new data, transformed into the data series
    // format Flot expects
    graph.update(data.toFlotData());

    // Update the last updated display
    setLastUpdatedBy(data.getLastUpdatedBy());

    // If we're still running schedule this method to be executed again at the
    // next interval
    if (running) {
      setTimeout(arguments.callee, updateIntervalInMillis);
    }
  }

  /**
   * Set the page description header.
   *
   * @param {string}
   *          desc Page description.
   */
  var setDescription = function(desc) {
    $("#description").text(desc);
  }

  /**
   * Set the last updated label, if one is provided.
   *
   * @param {string}
   *          s The new host that last updated our count data. If one is not
   *          provided the last updated label will not be shown.
   */
  var setLastUpdatedBy = function(s) {
    var message = s ? "Data last updated by: " + s : "";
    $("#updatedBy").text(message);
  }

  return {
    /**
     * Set the active resource the graph is displaying counts for. This is for
     * debugging purposes.
     *
     * @param {string}
     *          resource The resource to query our data provider for counts of.
     */
    setActiveResource : function(resource) {
      activeResource = resource;
      data.removeDataOlderThan((new Date()).getTime());
    },

    /**
     * Decorate the page. This will update various UI elements with dynamically
     * calculated values.
     */
    decorate : function() {
      setDescription("This graph displays the last "
          + graph.getTotalDurationToGraphInSeconds()
          + " seconds of counts as calculated by the Amazon Kinesis Data Visualization Sample Application.");
      $("#topNDescription").text(
          "Top " + topNToCalculate + " referrers by counts (Updated every "
              + (intervalsPerTopNUpdate * updateIntervalInMillis) + "ms):");
    },

    /**
     * Starts updating the graph at our defined interval.
     */
    start : function() {
      setDescription("Loading data...");
      var _this = this;
      // Load an initial range of data, decorate the page, and start the update polling process.
      updateData(activeResource, rangeOfDataToFetchEveryIntervalInSeconds,
          function() {
            // Decorate again now that we're done with the initial load
            _this.decorate();
            // Start our polling update
            running = true;
            update();
          });
    },

    /**
     * Stop updating the graph.
     */
    stop : function() {
      running = false;
    }
  }
};

/**
 * Provides easy access to count data.
 */
var CountDataProvider = function() {
  var _endpoint = "http://" + location.host + "/api/GetCounts";

  /**
   * Builds a URL to fetch the number of counts for a given resource in the past
   * range_in_seconds seconds.
   *
   * @param {string}
   *          resource The resource to request counts for.
   * @param {number}
   *          range_in_seconds The range in seconds, since now, to request
   *          counts for.
   *
   * @returns The URL to send a request for new data to.
   */
  buildUrl = function(resource, range_in_seconds) {
    return _endpoint + "?resource=" + resource + "&range_in_seconds="
        + range_in_seconds;
  };

  return {
    /**
     * Set the endpoint to request counts with.
     */
    setEndpoint : function(endpoint) {
      _endpoint = endpoint;
    },

    /**
     * Requests new data and passed it to the callback provided. The data is
     * expected to be returned in the following format. Note: Referrer counts
     * are ordered in descending order so the natural Top N can be derived per
     * interval simply by using the first N elements of the referrerCounts
     * array.
     *
     * [{
     *   "resource" : "/index.html",
     *   "timestamp" : 1397156430562,
     *   "host" : "worker01-ec2",
     *   "referrerCounts" : [
     *     {"referrer":"http://www.amazon.com","count":1002},
     *     {"referrer":"http://aws.amazon.com","count":901}
     *   ]
     * }]
     *
     * @param {string}
     *          resource The resource to request counts for.
     * @param {number}
     *          range_in_seconds The range in seconds, since now, to request
     *          counts for.
     * @param {function}
     *          callback The function to call when data has been returned from
     *          the endpoint.
     */
    getData : function(resource, range_in_seconds, callback) {
      $.ajax({
        url : buildUrl(resource, range_in_seconds)
      }).done(callback);
    }
  }
}

/**
 * Internal representation of count data. The data is stored in an associative
 * array by timestamp so it's easy to update a range of data without having to
 * manually deduplicate entries. The internal representation is then transformed
 * to what Flot expects with toFlotData().
 */
var CountData = function() {
  // Data format:
  // {
  //   "http://www.amazon.com" : {
  //     "label" : "http://www.amazon.com",
  //     "lastUpdatedBy" : "worker01-ec2"
  //     "data" : {
  //       "1396559634129" : 150
  //     }
  //   }
  // }
  var data = {};

  // Totals format:
  // {
  //   "http://www.amazon.com" : 102333
  // }
  var totals = {};

  // What host last updated the counts? This is useful to visualize how failover
  // happens when a worker is replaced.
  var lastUpdatedBy;

  /**
   * Update the total count for a given referrer.
   *
   * @param {string}
   *          referrer Referrer to update the total for.
   */
  var updateTotal = function(referrer) {
    // Simply loop through all the counts and sum them if there is data for this
    // referrer
    if (data[referrer]) {
      totals[referrer] = 0;
      $.each(data[referrer].data, function(ts, count) {
        totals[referrer] += count;
      });
    } else {
      // No data for the referrer, remove the total if it exists
      delete totals[referrer];
    }
  }

  /**
   * Set the host that last updated data.
   *
   * @param {string}
   *          host The host that last provided update counts.
   */
  var setLastUpdatedBy = function(host) {
    lastUpdatedBy = host;
  }

  return {
    /**
     * @returns {object} The internal representation of referrer data.
     */
    getData : function() {
      return data;
    },

    /**
     * @returns {string} The host that last updated our count data.
     */
    getLastUpdatedBy : function() {
      return lastUpdatedBy;
    },

    /**
     * @returns {object} An associative array of referrers to their total
     *          counts.
     */
    getTotals : function() {
      return totals;
    },

    /**
     * Compute local top N using the entire range of data we currently have.
     *
     * @param {number}
     *          n The number of top referrers to calculate.
     *
     * @returns {object[]} The top referrers by count in descending order.
     */
    getTopN : function(n) {
      // Create an array out of the totals so we can sort it
      var totalsAsArray = $.map(totals, function(count, referrer) {
        return {
          'referrer' : referrer,
          'count' : count
        };
      });
      // Sort descending by count
      var sorted = totalsAsArray.sort(function(a, b) {
        return b.count - a.count;
      });
      // Return the first N
      return sorted.slice(0, Math.min(n, sorted.length));
    },

    /**
     * Merges new count data in to our existing data set.
     *
     * @param {object} Count data returned by our data provider.
     */
    addNewData : function(newCountData) {
      // Expected data format:
      // [{
      //   "resource" : "/index.html",
      //   "timestamp" : 1397156430562,
      //   "host" : "worker01-ec2",
      //   "referrerCounts" : [{"referrer":"http://www.amazon.com","count":1002}]
      // }]
      newCountData.forEach(function(count) {
        // Update the host who last calculated the counts
        setLastUpdatedBy(count.host);
        // Add individual referrer counts
        count.referrerCounts.forEach(function(refCount) {
          // Reuse or create a new data series entry for this referrer
          refData = data[refCount.referrer] || {
            label : refCount.referrer,
            data : {}
          };
          // Set the count
          refData.data[count.timestamp] = refCount.count;
          // Update the referrer data
          data[refCount.referrer] = refData;
          // Update our totals whenever new data is added
          updateTotal(refCount.referrer);
        });
      });
    },

    /**
     * Removes data older than a specific time. This will also prune referrers
     * that have no data points.
     *
     * @param {number}
     *          timestamp Any data older than this time will be removed.
     */
    removeDataOlderThan : function(timestamp) {
      // For each referrer
      $.each(data, function(referrer, referrerData) {
        var shouldUpdateTotals = false;
        // For each data point
        $.each(referrerData.data, function(ts, count) {
          // If the data point is older than the provided time
          if (ts < timestamp) {
            // Remove the timestamp from the data
            delete referrerData.data[ts];
            // Indicate we need to update the totals for the referrer since we
            // removed data
            shouldUpdateTotals = true;
            // If the referrer has no more data remove the referrer entirely
            if (Object.keys(referrerData.data).length == 0) {
              // Remove the empty referrer - it has no more data
              delete data[referrer];
            }
          }
        });
        if (shouldUpdateTotals) {
          // Update the totals if we removed any data
          updateTotal(referrer);
        }
      });
    },

    /**
     * Convert our internal data to a Flot data object.
     *
     * @returns {object[]} Array of data series for every referrer we know of.
     */
    toFlotData : function() {
      flotData = [];
      $.each(data, function(referrer, referrerData) {
        flotData.push({
          label : referrer,
          // Flot expects time series data to be in the format:
          // [[timestamp as number, value]]
          data : $.map(referrerData.data, function(count, ts) {
            return [ [ parseInt(ts), count ] ];
          })
        });
      });
      return flotData;
    }
  }
}

var data = new CountData();
var provider = new CountDataProvider();
var graph = new Graph();
var uiHelper = new UIHelper(data, graph);
