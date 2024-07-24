package org.killbill.billing.plugin.meter.mapper;

import org.killbill.billing.plugin.meter.entity.RawUsage;
import org.skife.jdbi.v2.tweak.ResultSetMapper;
import org.skife.jdbi.v2.StatementContext;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RawUsageMapper implements ResultSetMapper<RawUsage> {

    public static final String TENANT_ID = "tenant_id";
    public static final String RECORD_DATE = "record_date";
    public static final String SUBSCRIPTION_ID = "subscription_id";
    public static final String UNIT_TYPE = "unit_type";
    public static final String AGGREGATED_ID = "aggregated_id";
    public static final String ID = "id";
    public static final String AMOUNT = "amount";

    public static final String CHARGES = "charges";

    private static final String TRACKING_ID = "tracking_id";

    @Override
    public RawUsage map(final int index, final ResultSet r, final StatementContext ctx) throws SQLException {
        final RawUsage rawUsage = new RawUsage();
        try {
            r.findColumn(TENANT_ID);
            rawUsage.setTenantId(r.getString(TENANT_ID));
        } catch (final SQLException ignored) {}

        try {
            r.findColumn(RECORD_DATE);
            rawUsage.setRecordDate(r.getString(RECORD_DATE));
        } catch (final SQLException ignored) {}


        try {
            r.findColumn(SUBSCRIPTION_ID);
            rawUsage.setSubscriptionId(r.getString(SUBSCRIPTION_ID));
        } catch (final SQLException ignored) {}

        try {
            r.findColumn(UNIT_TYPE);
            rawUsage.setUnitType(r.getString(UNIT_TYPE));
        } catch (final SQLException ignored) {
        }

        try {
            r.findColumn(AGGREGATED_ID);
            rawUsage.setAggregationId(r.getString(AGGREGATED_ID));
        } catch (final SQLException ignored) {}

        try {
            r.findColumn(ID);
            rawUsage.setId(Long.valueOf(r.getString(ID)));
        } catch (final SQLException ignored) {}

        try {
            r.findColumn(AMOUNT);
            rawUsage.setAmount(new BigDecimal(r.getString(AMOUNT)));
        } catch (final SQLException ignored) {}

        try {
            r.findColumn(TRACKING_ID);
            rawUsage.setTrackingId(r.getString(TRACKING_ID));
        } catch (final SQLException ignored) {}

        try {
            r.findColumn(CHARGES);
            rawUsage.setCharges(new BigDecimal(r.getString(CHARGES)));
        } catch (final SQLException ignored) {}

        return rawUsage;
    }
}
