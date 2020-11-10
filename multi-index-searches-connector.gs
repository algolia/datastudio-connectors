var cc = DataStudioApp.createCommunityConnector();

function isAdminUser() {
  return true;
}

function getAuthType() {
  var AuthTypes = cc.AuthType;
  return cc
    .newAuthTypeResponse()
    .setAuthType(AuthTypes.NONE)
    .build();
}

// https://devsite.googleplex.com/datastudio/connector/reference#getconfig
function getConfig() {
  var config = cc.getConfig();

  config
    .newTextInput()
    .setId("appID")
    .setName(
      "Enter AppID"
    );

  config
    .newTextInput()
    .setId("apiKey")
    .setName(
      "Enter the API Key (with analytics and listIndices rights)"
    );

  config
    .newTextInput()
    .setId("regex")
    .setName(
      "Enter the index name or index pattern ex: prod_products_*"
    );

  config
    .newTextInput()
    .setId("region")
    .setName(
      "Enter the analytics region (us or de)"
    );

  config.setDateRangeRequired(true);

  return config.build();
}

function getFields() {
  var fields = cc.getFields();
  var types = cc.FieldType;
  var aggregations = cc.AggregationType;

  fields
    .newDimension()
    .setId("index")
    .setName("Index")
    .setType(types.TEXT);
  
  fields
    .newDimension()
    .setId("search")
    .setName("Search")
    .setType(types.TEXT);

  fields
    .newMetric()
    .setId("count")
    .setName("Count")
    .setType(types.NUMBER);

  fields
    .newMetric()
    .setId("nbHits")
    .setName("Nb Hits")
    .setType(types.NUMBER);

  fields
    .newMetric()
    .setId("clickThroughRate")
    .setName("CTR")
    .setType(types.PERCENT);

  fields
    .newMetric()
    .setId("averageClickPosition")
    .setName("Average Click Position")
    .setType(types.NUMBER);

  fields
    .newMetric()
    .setId("conversionRate")
    .setName("Conversion Rate")
    .setType(types.PERCENT);

  fields
    .newMetric()
    .setId("trackedSearchCount")
    .setName("Tracked Searches Count")
    .setType(types.NUMBER);

  fields
    .newMetric()
    .setId("clickCount")
    .setName("Click Count")
    .setType(types.NUMBER);

  fields
    .newMetric()
    .setId("conversionCount")
    .setName("Conversion Count")
    .setType(types.NUMBER);

  fields
    .newMetric()
    .setId("noResultsCount")
    .setName("No Results Count")
    .setType(types.NUMBER);

  return fields;
}

// https://devsite.googleplex.com/datastudio/connector/reference#getschema
function getSchema(request) {
  return { schema: getFields().build() };
}

// https://devsite.googleplex.com/datastudio/connector/reference#getdata
function getData(request) {
  var requestedFields = getFields().forIds(
    request.fields.map(function(field) {
      return field.name;
    })
  );

  try {
    var indicesResponse = fetchIndices(request);
    var indices = normalizeResponse(request, indicesResponse);
    var selectedIndices = filterIndices(request, indices);
    var normalizedResponses = multiIndexFetchCountsAndClicksFromApi(request, selectedIndices);

    var data = getFormattedData(normalizedResponses, requestedFields);
  } catch (e) {
    cc.newUserError()
      .setDebugText("Error fetching data from API. Exception details: " + e)
      .setText(
        "The connector has encountered an unrecoverable error. Please try again later, or file an issue if this error persists."
      )
      .throwException();
  }

  return {
    schema: requestedFields.build(),
    rows: data
  };
}

/**
 * Filter indices according to the regex
 *
 * @param {Object} request Data request parameters.
 * @returns {string} Response text for UrlFetchApp.
 */
function filterIndices(request, indices) {
  var formattedRegex = request.configParams.regex.replace('.*','*') //To prevent * without point and ..* replacement
  var re = new RegExp("^"+formattedRegex.replace('*','.*')+"$");
  var filteredIndices = indices.items.reduce(function (acc, elt) {
    if (re.test(elt.name))
      acc.push(elt.name)
    return acc;
  }, [])
  return filteredIndices;
}

/**
 * List indices
 *
 * @param {Object} request Data request parameters.
 * @returns {string} Response text for UrlFetchApp.
 */
function fetchIndices(request) {
  var url = "https://" + request.configParams.appID + "-dsn.algolia.net/1/indexes";
  var params = {
    headers: {
      "X-Algolia-Application-Id": request.configParams.appID,
      "X-Algolia-API-Key": request.configParams.apiKey
    }
  };
  var response = UrlFetchApp.fetch(url, params);
  return response;
}

/**
 * Gets response for UrlFetchApp.
 *
 * @param {Object} request Data request parameters.
 * @param {List[string]}} selectedIndices All the indices needed
 * @returns {string} Response text for UrlFetchApp.
 */
function multiIndexFetchCountsAndClicksFromApi(request, selectedIndices) {
  var responses = selectedIndices.map(function(indexName) {
    const url = [
      "https://analytics.",
      request.configParams.region,
      ".algolia.com/2/searches",
      "?clickAnalytics=true&index=",
      indexName,
      "&startDate=",
      request.dateRange.startDate,
      "&endDate=",
      request.dateRange.endDate,
      "&limit=1000"
    ].join("");
    const params = {
      headers: {
        "X-Algolia-Application-Id": request.configParams.appID,
        "X-Algolia-API-Key": request.configParams.apiKey
      }
    };
    var response = UrlFetchApp.fetch(url, params);
    response = JSON.parse(response);
    return {index:indexName, payload:response};
  });
  return responses;
}

/**
 * Parses response string into an object.
 *
 * @param {Object} request Data request parameters.
 * @param {string} responseString Response from the API.
 * @return {Object} Contains package names as keys and associated download count
 *     information(object) as values.
 */
function normalizeResponse(request, responseString) {
  var response = JSON.parse(responseString);
  return response;
}

/**
 * Formats the parsed response from external data source into correct tabular
 * format and returns only the requestedFields
 *
 * @param {Array[Object]} responses The responses from external data source
 *     parsed into an object in a standard format.
 * @param {Array} requestedFields The fields requested in the getData request.
 * @returns {Array} Array containing rows of data in key-value pairs for each
 *     field.
 */
function getFormattedData(responses, requestedFields) {
  var data = responses.reduce(function(acc, response) {
    if (response.payload.searches !== null) {
      response.payload.searches.forEach(function(el) {
        var formattedData = formatData(requestedFields,
                                   response.index,
                                   el.search,
                                   el.count,
                                   el.nbHits,
                                   el.clickThroughRate,
                                   el.averageClickPosition,
                                   el.conversionRate,
                                   el.trackedSearchCount,
                                   el.clickCount,
                                   el.conversionCount,
                                   0);
        acc = acc.concat(formattedData);
      });
    }
    return acc;
  },[]);

  return data;
}

/**
 * Formats a single row of data into the required format.
 *
 * @param {Object} requestedFields Fields requested in the getData request.
 * @param {string} packageName Name of the package who's download data is being
 *    processed.
 * @param {Object} dailyDownload Contains the download data for a certain day.
 * @returns {Object} Contains values for requested fields in predefined format.
 */
function formatData(requestedFields,
                    index,
                    search,
                    count,
                    nbHits,
                    clickThroughRate,
                    averageClickPosition,
                    conversionRate,
                    trackedSearchCount,
                    clickCount,
                    conversionCount,
                    noResultsRate) {
  var row = requestedFields.asArray().map(function(requestedField) {
    switch (requestedField.getId()) {
      case "index":
        return index;
      case "search":
        return search;
      case "count":
        return count;
      case "nbHits":
        return nbHits;
      case "clickThroughRate":
        return clickThroughRate;
      case "averageClickPosition":
        return averageClickPosition;
      case "conversionRate":
        return conversionRate;
      case "trackedSearchCount":
        return trackedSearchCount;
      case "clickCount":
        return clickCount;
      case "conversionCount":
        return conversionCount;
      case "noResultsCount":
        return noResultsRate;
      default:
        return "";
    }
  });
  return { values: row };
}