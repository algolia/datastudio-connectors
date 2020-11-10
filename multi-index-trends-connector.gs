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
    .setId('day')
    .setName('Date')
    .setType(types.YEAR_MONTH_DAY);
  
  fields
    .newMetric()
    .setId("count")
    .setName("Search count")
    .setType(types.NUMBER);
  
  fields
    .newMetric()
    .setId("noResultRate")
    .setName("No result rate")
    .setType(types.PERCENT);
  
  fields
    .newMetric()
    .setId("noResultCount")
    .setName("No result count")
    .setType(types.NUMBER);
  
  fields
    .newMetric()
    .setId("trackedSearchCount")
    .setName("Tracked searches")
    .setType(types.NUMBER);
  
  fields
    .newMetric()
    .setId("clickThroughRate")
    .setName("CTR")
    .setType(types.PERCENT);
  
  fields
    .newMetric()
    .setId("clickCount")
    .setName("Click count")
    .setType(types.NUMBER);
  
  fields
    .newMetric()
    .setId("averageClickPosition")
    .setName("Average click position")
    .setType(types.NUMBER);
  
  fields
    .newMetric()
    .setId("conversionRate")
    .setName("Conversion rate")
    .setType(types.PERCENT);
  
  fields
    .newMetric()
    .setId("conversionCount")
    .setName("Conversion count")
    .setType(types.NUMBER);

  return fields;
}

function getSchema(request) {
  return { schema: getFields().build() };
}

function getData(request) {
  var requestedFields = getFields().forIds(
    request.fields.map(function(field) {
      return field.name;
    })
  );

  try {
    var apiResponse_indices = fetchIndices(request)
    var indices = normalizeResponse(request, apiResponse_indices),
    indices = filterIndices(request, indices)
    
    var allData = indices.reduce(function(acc, indexName) {
      var data = getIndexInsights(request, indexName, requestedFields);
      acc = acc.concat(data);
      return acc;
    },[]);
    
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
    rows: allData
  };
}

function getIndexInsights(request, indexName, requestedFields) {
  var apiResponse_count = fetchAlgoliaAPI(request, "searches/count", indexName);
  var apiResponse_noResult = fetchAlgoliaAPI(request, "searches/noResultRate", indexName);
  var apiResponse_CTR = fetchAlgoliaAPI(request, "clicks/clickThroughRate", indexName);
  var apiResponse_avgClickPos = fetchAlgoliaAPI(request, "clicks/averageClickPosition", indexName);
  var apiResponse_CR = fetchAlgoliaAPI(request, "conversions/conversionRate", indexName);
  
  var data = getFormattedData(
    indexName,
    normalizeResponse(request, apiResponse_count),
    normalizeResponse(request, apiResponse_noResult),
    normalizeResponse(request, apiResponse_CTR),
    normalizeResponse(request, apiResponse_avgClickPos),
    normalizeResponse(request, apiResponse_CR),
    requestedFields);
  return data;
}

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

function buildUrl(endpoint, region, index, startDate, endDate) {
  return [
    "https://analytics.",
    region,
    ".algolia.com/2/",
    endpoint,
    "?index=",
    index,
    "&startDate=",
    startDate,
    "&endDate=",
    endDate
  ].join("");
}

function fetchAlgoliaAPI(request, endpoint, indexName) {
  var cache = CacheService.getScriptCache();
  var url = buildUrl(
    endpoint,
    request.configParams.region,
    indexName,
    request.dateRange.startDate,
    request.dateRange.endDate);
  
  var cached = cache.get(url)
  if (cached != null) {
    return cached;
  }
  
  var params = {
    headers: {
      "X-Algolia-Application-Id": request.configParams.appID,
      "X-Algolia-API-Key": request.configParams.apiKey
    }
  };
  var response = UrlFetchApp.fetch(url, params);
  cache.put(url, response, 3600); //Cache 1 hour
  return response;
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
        
function getFormattedData(indexName, responseCount, responseNoResult, responseCTR, responseAverageClickPosition, responseCR, requestedFields) {  
  var mergedData = responseCount.dates.reduce(function(acc, el, index) {
    acc.push({
      "date": el.date,
      "count": el.count,
      "noResultRate": responseNoResult.dates[index].rate,
      "noResultCount": responseNoResult.dates[index].noResultCount,
      "trackedSearchCount": responseCTR.dates[index].trackedSearchCount,
      "clickCount": responseCTR.dates[index].clickCount,
      "clickThroughRate": responseCTR.dates[index].rate,
      "averageClickPosition": responseAverageClickPosition.dates[index].average,
      "conversionRate": responseCR.dates[index].rate,
      "conversionCount": responseCR.dates[index].conversionCount,
    });
    return acc;
  }, []);
  
  var data = mergedData.reduce(function(acc, el) {
    var formattedData = formatData(requestedFields,
                                   indexName,
                                   el.date,
                                   el.count,
                                   el.noResultRate,
                                   el.noResultCount,
                                   el.trackedSearchCount,
                                   el.clickThroughRate,
                                   el.clickCount,
                                   el.averageClickPosition,
                                   el.conversionRate,
                                   el.conversionCount);
    acc = acc.concat(formattedData);
    return acc;
  }, []);
  
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
                    indexName,
                    date,
                    count,
                    noResultRate,
                    noResultCount,
                    trackedSearchCount,
                    clickThroughRate,
                    clickCount,
                    averageClickPosition,
                    conversionRate,
                    conversionCount) {
  var row = requestedFields.asArray().map(function(requestedField) {
    switch (requestedField.getId()) {
      case "index":
        return indexName;
      case "day":
        return date.replace("-","").replace("-","");
      case "count":
        return count;
      case "noResultRate":
        return noResultRate;
      case "noResultCount":
        return noResultCount;
      case "trackedSearchCount":
        return trackedSearchCount;
      case "clickThroughRate":
        return clickThroughRate;
      case "clickCount":
        return clickCount;
      case "averageClickPosition":
        return averageClickPosition;
      case "conversionRate":
        return conversionRate;
      case "conversionCount":
        return conversionCount;
      default:
        return "";
    }
  });
  return { values: row };
}