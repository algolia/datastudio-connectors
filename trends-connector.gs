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
      "Enter the Analytics API Key"
    );
  
  config
    .newTextInput()
    .setId("index")
    .setName(
      "Enter the index name"
    );
  
  config
    .newTextInput()
    .setId("region")
    .setName(
      "Enter the analytics region"
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
    var apiResponse_count = fetchCount(request);
    var apiResponse_noResult = fetchNoResult(request);
    var apiResponse_CTR = fetchClickThroughRate(request);
    var apiResponse_avgClickPos = fetchAverageClickPosition(request);
    var apiResponse_CR = fetchConversionRate(request);
    
    var data = getFormattedData(
      normalizeResponse(request, apiResponse_count),
      normalizeResponse(request, apiResponse_noResult),
      normalizeResponse(request, apiResponse_CTR),
      normalizeResponse(request, apiResponse_avgClickPos),
      normalizeResponse(request, apiResponse_CR),
      requestedFields);
    
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

function buildUrl(appID, apiKey, endpoint, region, index, startDate, endDate) {
  var url = [
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
  var params = {
    headers: {
      "X-Algolia-Application-Id": appID,
      "X-Algolia-API-Key": apiKey
    }
  };
  return UrlFetchApp.fetch(url, params);
}

function fetchCount(request) {
  var response = buildUrl(
    request.configParams.appID,
    request.configParams.apiKey,
    "searches/count",
    request.configParams.region,
    request.configParams.index,
    request.dateRange.startDate,
    request.dateRange.endDate);
  return response;
}

function fetchNoResult(request) {
  var response = buildUrl(
    request.configParams.appID,
    request.configParams.apiKey,
    "searches/noResultRate",
    request.configParams.region,
    request.configParams.index,
    request.dateRange.startDate,
    request.dateRange.endDate);
  return response;
}

function fetchClickThroughRate(request) {
  var response = buildUrl(
    request.configParams.appID,
    request.configParams.apiKey,
    "clicks/clickThroughRate",
    request.configParams.region,
    request.configParams.index,
    request.dateRange.startDate,
    request.dateRange.endDate);
  return response;
}

function fetchAverageClickPosition(request) {
  var response = buildUrl(
    request.configParams.appID,
    request.configParams.apiKey,
    "clicks/averageClickPosition",
    request.configParams.region,
    request.configParams.index,
    request.dateRange.startDate,
    request.dateRange.endDate);
  return response;
}

function fetchConversionRate(request) {
  var response = buildUrl(
    request.configParams.appID,
    request.configParams.apiKey,
    "conversions/conversionRate",
    request.configParams.region,
    request.configParams.index,
    request.dateRange.startDate,
    request.dateRange.endDate);
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
        
function getFormattedData(responseCount, responseNoResult, responseCTR, responseAverageClickPosition, responseCR, requestedFields) {
  var mergedData = [];
  responseCount.dates.forEach(function(el) {
    mergedData.push({
      "date": el.date,
      "count": el.count
    });
  });
  
  responseNoResult.dates.forEach(function(el) {
    var dateObj = mergedData.filter(function(dateElement) {
      return dateElement.date == el.date;
    });
    if (dateObj.length > 0) {
      dateObj[0].noResultRate = el.rate;
      dateObj[0].noResultCount = el.noResultCount;
    }
  });
  
  responseCTR.dates.forEach(function(el) {
    var dateObj = mergedData.filter(function(dateElement) {
      return dateElement.date == el.date;
    });
    if (dateObj.length > 0) {
      dateObj[0].trackedSearchCount = el.trackedSearchCount;
      dateObj[0].clickThroughRate = el.rate;
      dateObj[0].clickCount = el.conversionCount;
    }
  });
  
  responseAverageClickPosition.dates.forEach(function(el) {
    var dateObj = mergedData.filter(function(dateElement) {
      return dateElement.date == el.date;
    });
    if (dateObj.length > 0) {
      dateObj[0].averageClickPosition = el.average;
    }
  });
  
  responseCR.dates.forEach(function(el) {
    var dateObj = mergedData.filter(function(dateElement) {
      return dateElement.date == el.date;
    });
    if (dateObj.length > 0) {
      dateObj[0].conversionRate = el.rate;
      dateObj[0].conversionCount = el.conversionCount;
    }
  });
  
  var data = [];
  mergedData.forEach(function(el) {
    var formattedData = formatData(requestedFields,
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
    data = data.concat(formattedData);
  });
  
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