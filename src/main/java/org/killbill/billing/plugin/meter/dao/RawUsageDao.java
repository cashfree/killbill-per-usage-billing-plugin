/*
 * Copyright 2020-2024 Equinix, Inc
 * Copyright 2014-2024 The Billing Project, LLC
 *
 * The Billing Project licenses this file to you under the Apache License, version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at:
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package org.killbill.billing.plugin.meter.dao;

import java.sql.SQLException;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import javax.sql.DataSource;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.killbill.billing.plugin.meter.dto.AggregatedUsage;
import org.killbill.billing.plugin.meter.dto.InvoiceTenant;
import org.killbill.billing.plugin.meter.entity.RawUsage;
import org.killbill.billing.plugin.meter.exception.ResourceNotFoundException;
import org.killbill.billing.plugin.meter.mapper.InvoiceTenantMapper;
import org.killbill.billing.plugin.meter.mapper.RawUsageMapper;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.PreparedBatch;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RawUsageDao {

     private final DBI dbi;
     public RawUsageDao(DataSource dataSource){
          dbi=new DBI(dataSource);
     }
     public void insertRawUsage(RawUsage rawUsage){
         try (final Handle h = dbi.open()) {
             h.createStatement("INSERT INTO `raw_usage` (\n" +
                               "  `tenant_id`, \n" +
                               "  `subscription_id`, \n" +
                               "  `tracking_id`, \n" +
                               "  `unit_type`, \n" +
                               "  `record_date`, \n" +
                               "  `amount`, \n" +
                               "  `charges`, \n" +
                               "  `version` \n" +
                               ") VALUES (\n" +
                               "  :tenantId, \n" +
                               "  :subscriptionId, \n" +
                               "  :trackingId, \n" +
                               "  :unitType, \n" +
                               "  :recordDate, \n" +
                               "  :amount, \n" +
                               "  :charges, \n" +
                               "  :version \n"+
                               ")")
              .bind("tenantId", rawUsage.getTenantId())
              .bind("subscriptionId", rawUsage.getSubscriptionId())
              .bind("trackingId", rawUsage.getTrackingId())
              .bind("unitType", rawUsage.getUnitType())
              .bind("recordDate", rawUsage.getRecordDate())
              .bind("amount", rawUsage.getAmount())
              .bind("charges", rawUsage.getCharges())
              .bind("version", 0)
              .execute();
         } catch (final Exception e) {
             log.error("Error :: {}", e.getMessage());
             throw e;
         }
     }

    public List<RawUsage> getUniqueUnAggregatedUsage() {
        try (final Handle handle = dbi.open()) {
            return handle.createQuery("select distinct `tenant_id`, `subscription_id`, `unit_type` from `raw_usage` " +
                                      "where `charges` is NULL and `aggregated_id` is NULL")
                         .map(new RawUsageMapper())
                         .list();
        } catch (final Exception e) {
            log.error("Error :: {}", e.getMessage());
            throw e;
        }
    }

    public void fillAggregationId(final RawUsage rawUsage, final UUID uuid,final UUID prev) {
        try (final Handle handle = dbi.open()) {
            final String sql = "UPDATE raw_usage " +
                               "SET aggregated_id = CASE " +
                               "WHEN DATE(record_date) = CURDATE() THEN :aggregationId " +
                               "ELSE :prevId " +
                               "END " +
                               "WHERE tenant_id = :tenantId " +
                               "AND subscription_id = :subscriptionId " +
                               "AND unit_type = :unitType " +
                               "AND aggregated_id IS NULL";

            handle.createStatement(sql)
                  .bind("aggregationId", String.valueOf(uuid))
                  .bind("tenantId", rawUsage.getTenantId())
                  .bind("subscriptionId", rawUsage.getSubscriptionId())
                  .bind("unitType", rawUsage.getUnitType())
                  .bind("prevId", String.valueOf(prev))
                  .execute();
        } catch (final Exception e) {
            log.error("Error :: {}", e.getMessage());
            throw e;
        }
    }

    public List<RawUsage> getUnbilledAggregationIds() {
        try (final Handle handle = dbi.open()) {
            final String sql = "select distinct `aggregated_id`, `subscription_id` , `tenant_id`, `unit_type` from `raw_usage`" +
                               "where `charges` is NULL and `aggregated_id` is NOT NULL";

            return handle.createQuery(sql)
                         .map(new RawUsageMapper())
                         .list();
        } catch (final Exception e) {
            log.error("Error :: {}", e.getMessage());
            throw e;
        }
    }

    public AggregatedUsage getUsageSum(final RawUsage subscriptionUsage) {
            try (final Handle handle = dbi.open()) {
                final String sql = "SELECT SUM(amount) AS total_amount, MAX(record_date) AS max_record_date FROM raw_usage WHERE aggregated_id = :aggregationId ";

                return handle.createQuery(sql)
                             .bind("aggregationId", subscriptionUsage.getAggregationId())
                             .map((index, r, ctx) -> {
                                 // Check if the column "total_amount" is present
                                 final AggregatedUsage aggregatedUsage=new AggregatedUsage();
                                 try {
                                     r.findColumn("total_amount");
                                     aggregatedUsage.setSum(r.getBigDecimal("total_amount"));
                                     r.findColumn("max_record_date");
                                     final DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
                                     final DateTime dateTime = DateTime.parse(r.getString("max_record_date"), formatter);
                                     aggregatedUsage.setMaxRecordDate(dateTime);
                                     return aggregatedUsage;
                                 } catch (final SQLException e) {
                                     // Column "total_amount" is not present, return 0.0
                                     log.error("Error :: {}",e.getMessage());
                                     throw e;
                                 }
                             })
                             .first(); // We expect a single result
            } catch (final Exception e) {
                log.error("Error :: {}", e.getMessage());
                throw new RuntimeException(e);
            }
    }

    public List<InvoiceTenant> getNonBackfilledInvoices() {
        final String sql = "SELECT invoice_tracking_ids.invoice_id as invoice_id, tenants.id as tenant_id\n" +
                           "FROM invoice_tracking_ids \n" +
                           "JOIN tenants ON tenants.record_id = invoice_tracking_ids.tenant_record_id \n" +
                           "WHERE invoice_tracking_ids.tracking_id IN (\n" +
                           "    SELECT DISTINCT aggregated_id \n" +
                           "    FROM raw_usage \n" +
                           "    WHERE aggregated_id IS NOT NULL AND charges IS NULL\n" +
                           ");";
        try (final Handle handle = dbi.open()) {
            return handle.createQuery(sql)
                         .map(new InvoiceTenantMapper())
                         .list();
        } catch (final Exception e) {
            log.error("Error :: {}", e.getMessage());
            throw e;
        }
    }

    public List<RawUsage> getRawUsage(final List<String> trackingIds, final String unitType) {
        try (final Handle handle = dbi.open()) {
            String SQL = "SELECT id, amount, subscription_id , tracking_id , record_date FROM `raw_usage` " +
                         "WHERE aggregated_id IN (<trackingIds>) AND unit_type = :unitType";

            final String trackingIdsStr = trackingIds.stream()
                                               .map(id -> "'" + id + "'")
                                               .collect(Collectors.joining(", "));

            SQL = SQL.replace("<trackingIds>", trackingIdsStr);

            return handle.createQuery(SQL)
                         .bind("unitType", unitType)
                         .map(new RawUsageMapper())
                         .list();
        } catch (final Exception e) {
            log.error("Error :: {}", e.getMessage());
            throw e;
        }
    }

    public void persistRawUsageCharges(final Iterable<RawUsage> rawUsages) {
        try (final Handle handle = dbi.open()) {
            handle.begin();

            final String sql = "UPDATE raw_usage " +
                               "SET charges = :charges, " +
                               "tier = :tier " +
                               "WHERE id = :id ";

            PreparedBatch batch = handle.prepareBatch(sql);
            int batchSize = 0;

            for (final RawUsage rawUsage : rawUsages) {
                batch.bind("id", String.valueOf(rawUsage.getId()))
                     .bind("charges", rawUsage.getCharges())
                     .bind("tier", rawUsage.getTier())
                     .add();

                batchSize++;

                if (batchSize == 1000) {
                    batch.execute();
                    batch = handle.prepareBatch(sql);
                    batchSize = 0;
                }
            }

            if (batchSize > 0) {
                batch.execute();
            }

            handle.commit();
        } catch (final Exception e) {
            log.error("Error :: {}", e.getMessage());
            throw e;
        }
    }

    public RawUsage getRawUsage(final String tenantId, final String subscriptionId, final String unit, final String trackingId){
         final String sql = "SELECT * FROM `raw_usage`\n" +
                            "WHERE tenant_id = :tenantId \n" +
                            "and subscription_id = :subscriptionId \n" +
                            "and tracking_id= :trackingId \n" +
                            "AND unit_type= :unitType;";
        List<RawUsage> rawUsages = null;
        try (final Handle handle = dbi.open()) {
            rawUsages= handle.createQuery(sql)
                    .bind("tenantId",tenantId)
                    .bind("subscriptionId",subscriptionId)
                    .bind("trackingId",trackingId)
                    .bind("unitType",unit)
                    .map(new RawUsageMapper()).list();
            if(rawUsages.size() == 1){
                return rawUsages.get(0);
            }
        }
        catch (final Exception ignored){

        }
        throw new ResourceNotFoundException("Raw usage not found");
    }

    public RawUsage getRawUsage(final String tenantId, final List<String> subscriptionIds, final String unit, final String trackingId){
        String sql = "SELECT * FROM `raw_usage`\n" +
                           "WHERE tenant_id = :tenantId \n" +
                           "and subscription_id IN (<subscriptionId>) \n" +
                           "and tracking_id= :trackingId \n" +
                           "AND unit_type= :unitType;";

        final String trackingIdsStr = subscriptionIds.stream()
                                                 .map(id -> "'" + id + "'")
                                                 .collect(Collectors.joining(", "));

        sql = sql.replace("<subscriptionId>", trackingIdsStr);
        List<RawUsage> rawUsages = null;
        try (final Handle handle = dbi.open()) {
            rawUsages= handle.createQuery(sql)
                             .bind("tenantId",tenantId)
                             .bind("subscriptionId",subscriptionIds)
                             .bind("trackingId",trackingId)
                             .bind("unitType",unit)
                             .map(new RawUsageMapper()).list();
            if(rawUsages.size() == 1){
                return rawUsages.get(0);
            }
        }
        catch (final Exception ignored){

        }
        throw new ResourceNotFoundException("Raw usage not found");
    }
}
