package mmarkiew;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.services.analyticsreporting.v4.AnalyticsReporting;
import com.google.api.services.analyticsreporting.v4.AnalyticsReportingScopes;
import com.google.api.services.analyticsreporting.v4.model.ColumnHeader;
import com.google.api.services.analyticsreporting.v4.model.DateRange;
import com.google.api.services.analyticsreporting.v4.model.DateRangeValues;
import com.google.api.services.analyticsreporting.v4.model.GetReportsRequest;
import com.google.api.services.analyticsreporting.v4.model.GetReportsResponse;
import com.google.api.services.analyticsreporting.v4.model.Metric;
import com.google.api.services.analyticsreporting.v4.model.MetricHeaderEntry;
import com.google.api.services.analyticsreporting.v4.model.Report;
import com.google.api.services.analyticsreporting.v4.model.ReportRequest;
import com.google.api.services.analyticsreporting.v4.model.ReportRow;

import java.io.FileInputStream;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class GoogleAnalyticsReportDataProvider {

    private static final String APPLICATION_NAME = "Hello Analytics Reporting";
    private static final JsonFactory JSON_FACTORY = GsonFactory.getDefaultInstance();
    private static final String KEY_FILE_LOCATION = "/path/to/private/key.json";
    private static final String VIEW_ID = "ga:{TODO_ID_PASTE_HERE}";

    private static final String ALIAS_COUNT = "count";
    private static final String ALIAS_UNIQUE = "unique";

    private final AnalyticsReporting service;

    public static void main(String[] args) {
        try {
            GoogleAnalyticsReportDataProvider main = new GoogleAnalyticsReportDataProvider();

            System.out.println(main.countPageViews(null));
            System.out.println(main.countPageViews("/"));
            System.out.println(main.countDownloadEvents(null));
            System.out.println(main.countDownloadEvents("WUT87b2e7599be541a5861dc6d468b91866"));
            System.out.println(main.countDownloadEvents("WUT04db9990dbd64ffe909f41952402f78b"));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public GoogleAnalyticsReportDataProvider() throws GeneralSecurityException, IOException {
        HttpTransport httpTransport = GoogleNetHttpTransport.newTrustedTransport();
        GoogleCredential credential = GoogleCredential
                .fromStream(new FileInputStream(KEY_FILE_LOCATION))
                .createScoped(AnalyticsReportingScopes.all());

        service = new AnalyticsReporting.Builder(httpTransport, JSON_FACTORY, credential)
                .setApplicationName(APPLICATION_NAME).build();
    }

    public ViewsCount countPageViews(String path) throws IOException {
        ReportRequest request = pageRequest(defaultDateRange(), path);
        GetReportsResponse response = requestReport(request);
        debugPrintResponse(response);

        return parseResponseForRequestsFromThisClass(response);
    }

    public ViewsCount countDownloadEvents(String fileId) throws IOException {
        ReportRequest request = downloadEventsRequest(defaultDateRange(), fileId);
        GetReportsResponse response = requestReport(request);
        debugPrintResponse(response);

        return parseResponseForRequestsFromThisClass(response);
    }

    private ViewsCount parseResponseForRequestsFromThisClass(GetReportsResponse response) {
        ViewsCount viewsCount = new ViewsCount();

        Report report = response.getReports()
                .get(0);
        List<ReportRow> rows = report
                .getData()
                .getRows();
        if (rows == null) {
            System.out.println("No data found for " + VIEW_ID);
            return viewsCount;
        }

        List<MetricHeaderEntry> entries = report.getColumnHeader()
                .getMetricHeader()
                .getMetricHeaderEntries();
        List<String> values = rows.get(0)
                .getMetrics()
                .get(0)
                .getValues();

        for (int i = 0; i < values.size(); ++i) {
            if (ALIAS_COUNT.equals(entries.get(i).getName())) {
                viewsCount.count = Integer.valueOf(values.get(i));
            } else if (ALIAS_UNIQUE.equals(entries.get(i).getName())) {
                viewsCount.unique = Integer.valueOf(values.get(i));
            }
        }

        return viewsCount;
    }

    private GetReportsResponse requestReport(ReportRequest request) throws IOException {
        GetReportsRequest req = new GetReportsRequest()
                .setReportRequests(Collections.singletonList(request));

        long start = System.currentTimeMillis();
        GetReportsResponse response = service.reports()
                .batchGet(req)
                .execute();
        System.out.println("Request took: " + (System.currentTimeMillis() - start) + "ms");

        return response;
    }

    private static DateRange defaultDateRange() {
        DateRange dateRange = new DateRange();
        dateRange.setStartDate("2011-09-14");
        dateRange.setEndDate("today");

        return dateRange;
    }

    private static ReportRequest pageRequest(DateRange dateRange, String path) {
        Metric pageviews = new Metric().setExpression("ga:pageviews")
                .setAlias(ALIAS_COUNT);
        Metric uniquePageviews = new Metric().setExpression("ga:uniquePageviews")
                .setAlias(ALIAS_UNIQUE);

//        Dimension pagePath = new Dimension().setName("ga:PagePath");

        ReportRequest request = new ReportRequest()
                .setViewId(VIEW_ID)
                .setDateRanges(Collections.singletonList(dateRange))
//                .setDimensions(Collections.singletonList(pagePath))
                .setMetrics(Arrays.asList(pageviews, uniquePageviews));

        if (path != null) {
            request.setFiltersExpression("ga:pagePath==" + path);
        }

        return request;
    }

    private static ReportRequest downloadEventsRequest(DateRange dateRange, String fileId) {
        Metric totalEvents = new Metric()
                .setExpression("ga:totalEvents")
                .setAlias(ALIAS_COUNT);
        Metric uniqueEvents = new Metric().setExpression("ga:uniqueEvents")
                .setAlias(ALIAS_UNIQUE);

        StringBuilder expression = new StringBuilder("ga:eventCategory==files;ga:eventAction==download");
        if (fileId != null) {
            expression.append(";ga:eventLabel==");
            expression.append(fileId);
        }

        return new ReportRequest()
                .setViewId(VIEW_ID)
                .setFiltersExpression(expression.toString())
                .setDateRanges(Collections.singletonList(dateRange))
                .setMetrics(Arrays.asList(totalEvents, uniqueEvents));
    }

    public static class ViewsCount {
        private int count;
        private int unique;

        @Override
        public String toString() {
            return String.format("  count: %d, unique: %d", count, unique);
        }
    }

    private static void debugPrintResponse(GetReportsResponse response) {
        for (Report report : response.getReports()) {
            ColumnHeader header = report.getColumnHeader();
            List<String> dimensionHeaders = header.getDimensions();
            List<MetricHeaderEntry> metricHeaders = header.getMetricHeader().getMetricHeaderEntries();
            List<ReportRow> rows = report.getData().getRows();

            if (rows == null) {
                System.out.println("No data found for " + VIEW_ID);
                return;
            }

            for (ReportRow row : rows) {
                List<String> dimensions = row.getDimensions();
                List<DateRangeValues> metrics = row.getMetrics();

                if (dimensionHeaders != null)
                    for (int i = 0; i < dimensionHeaders.size() && i < dimensions.size(); i++) {
                        System.out.println(dimensionHeaders.get(i) + ": " + dimensions.get(i));
                    }

                for (int j = 0; j < metrics.size(); j++) {
                    System.out.print("Date Range (" + j + "): ");
                    DateRangeValues values = metrics.get(j);
                    for (int k = 0; k < values.getValues().size() && k < metricHeaders.size(); k++) {
                        System.out.println(metricHeaders.get(k).getName() + ": " + values.getValues().get(k));
                    }
                }
            }
        }
    }

}
