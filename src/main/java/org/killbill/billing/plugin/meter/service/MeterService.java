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

package org.killbill.billing.plugin.meter.service;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

import javax.inject.Singleton;

import org.joda.time.DateTime;
import org.joda.time.LocalDate;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.jooq.tools.StringUtils;
import org.killbill.billing.entitlement.api.Subscription;
import org.killbill.billing.invoice.api.Invoice;
import org.killbill.billing.invoice.api.InvoiceApiException;
import org.killbill.billing.invoice.api.InvoiceItem;
import org.killbill.billing.osgi.libs.killbill.OSGIKillbillAPI;
import org.killbill.billing.plugin.api.PluginCallContext;
import org.killbill.billing.plugin.meter.dao.RawUsageDao;
import org.killbill.billing.plugin.meter.domain.external.ChargeDetails;
import org.killbill.billing.plugin.meter.domain.external.ChargeEvent;
import org.killbill.billing.plugin.meter.domain.external.ConsumerSubscriptionUsageRecord;
import org.killbill.billing.plugin.meter.domain.external.ConsumerUnitUsageRecord;
import org.killbill.billing.plugin.meter.domain.external.ConsumerUsageRecord;
import org.killbill.billing.plugin.meter.dto.AggregatedUsage;
import org.killbill.billing.plugin.meter.dto.InvoiceTenant;
import org.killbill.billing.plugin.meter.dto.TierDetail;
import org.killbill.billing.plugin.meter.dto.TierDetails;
import org.killbill.billing.plugin.meter.entity.RawUsage;
import org.killbill.billing.plugin.meter.exception.RequestTooEarly;
import org.killbill.billing.usage.api.SubscriptionUsageRecord;
import org.killbill.billing.usage.api.UnitUsageRecord;
import org.killbill.billing.usage.api.UsageRecord;
import org.killbill.billing.util.callcontext.CallContext;
import org.killbill.billing.util.callcontext.CallOrigin;
import org.killbill.billing.util.callcontext.UserType;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.extern.slf4j.Slf4j;

import static org.killbill.billing.plugin.meter.MeterRegister.PLUGIN_NAME;

@Singleton
@Slf4j
public class MeterService {

    public static final String TIER_CHANGE_EVENT = "TIER_CHANGE";
    public static final String CHARGE = "CHARGE";
    public static final double TAX_RATE = 0.18;
    private final RawUsageDao rawUsageDao;

    protected OSGIKillbillAPI killbillAPI;

    private final ObjectMapper objectMapper;

    public MeterService(final RawUsageDao rawUsageDao, final OSGIKillbillAPI killbillAPI) {this.rawUsageDao = rawUsageDao;
        this.killbillAPI = killbillAPI;
        this.objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());
    }

    public void addRawUsage(final ConsumerSubscriptionUsageRecord request) {
        for(final ConsumerUnitUsageRecord consumerUnitUsageRecord: request.getUnitUsageRecords()){
            for(final ConsumerUsageRecord consumerUsageRecord: consumerUnitUsageRecord.getUsageRecords()){
                final RawUsage rawUsage= new RawUsage()
                                                 .setSubscriptionId(request.getSubscriptionId())
                                                 .setTrackingId(request.getTrackingId())
                                                 .setTenantId(String.valueOf(request.getTenantId()))
                                                 .setUnitType(consumerUnitUsageRecord.getUnitType())
                                                 .setRecordDate(convertToISOFormat(consumerUsageRecord.getRecordDate()))
                                                 .setAmount(consumerUsageRecord.getAmount());
                rawUsageDao.insertRawUsage(rawUsage);
            }
        }
    }

    public static String convertToISOFormat(DateTime dateTime) {
        DateTimeFormatter dateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS");
        return dateTime.toString(dateTimeFormatter);
    }
    public void aggregateUsages() {
        final List<RawUsage> uniqueUsages = rawUsageDao.getUniqueUnAggregatedUsage();
        log.info("MeterService :: raw usage {}",uniqueUsages.size());
        for(final RawUsage rawUsage:uniqueUsages){
            final UUID uuid=UUID.randomUUID();
            final UUID prev=UUID.randomUUID();
            log.info("aggregateUsages uuid :: {} {}",uuid,prev);
            rawUsageDao.fillAggregationId(rawUsage, uuid,prev);
        }
    }

    public void bill() {
        final List<RawUsage> uniqueSubsId = rawUsageDao.getUnbilledAggregationIds();
        log.info("bill {}",uniqueSubsId);
        uniqueSubsId.forEach(this::pushUsage);
    }

    private void pushUsage(final RawUsage subscriptionUsage) {
        try {
            final CallContext callContext = getCallContext(subscriptionUsage.getTenantId());
            final Subscription subscription = killbillAPI.getSubscriptionApi()
                           .getSubscriptionForExternalKey(subscriptionUsage.getSubscriptionId(),
                                                          false, callContext);
            final SubscriptionUsageRecord subscriptionUsageRecord = createSubscriptionUsageRecord(subscriptionUsage, subscription.getId());
            killbillAPI.getUsageUserApi().recordRolledUpUsage(subscriptionUsageRecord, callContext);
        } catch (final Exception e) {
            log.error("PushUsage :: {} :: {}",e.getClass(),e.getMessage());
        }
        log.info("consumeKafkaMessages :: usage recorded");
    }

    private PluginCallContext getCallContext(final String tenantId) {
        return new PluginCallContext(UUID.randomUUID(), PLUGIN_NAME,
                                     CallOrigin.INTERNAL, UserType.ADMIN, "Pushed from" + this.getClass().getSimpleName(), "Test" + this.getClass().getSimpleName(),
                                     DateTime.now(), DateTime.now(), null, UUID.fromString(tenantId));
    }

    private SubscriptionUsageRecord createSubscriptionUsageRecord(final RawUsage subscriptionUsage, final UUID id) {
        return new SubscriptionUsageRecord(id, subscriptionUsage.getAggregationId(),mapUsageRecord(subscriptionUsage));
    }

    private List<UnitUsageRecord> mapUsageRecord(final RawUsage subscriptionUsage) {
        final AggregatedUsage aggregatedUsage  =rawUsageDao.getUsageSum(subscriptionUsage);
        return List.of(new UnitUsageRecord(subscriptionUsage.getUnitType(),List.of(new UsageRecord(aggregatedUsage.getMaxRecordDate(),aggregatedUsage.getSum()))));
    }

    public void backFill() {
        final List<InvoiceTenant> invoiceList = rawUsageDao.getNonBackfilledInvoices();
        log.info("InvoiceTenant List :: {}",invoiceList);
        for(final InvoiceTenant invoiceTenant:invoiceList){
            try {
                final Invoice invoice = killbillAPI.getInvoiceUserApi().getInvoice(UUID.fromString(invoiceTenant.getInvoiceId()),getCallContext(invoiceTenant.getTenantId()));
                for (final InvoiceItem invoiceItem :invoice.getInvoiceItems()){
                    backFillCharges(invoice.getTrackingIds(),invoiceItem.getItemDetails(),invoiceTenant.getTenantId());
                }
            } catch (InvoiceApiException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private void backFillCharges(final List<String> trackingIds, final String itemDetails,final String tenant) {
        if(StringUtils.isBlank(itemDetails)){
            return;
        }
        final TierDetails tierDetails;
        try {
            tierDetails = objectMapper.readValue(itemDetails, TierDetails.class);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
        if(tierDetails.getTierDetails().size()==0){
            return;
        }
        final String tierUnit = tierDetails.getTierDetails().get(0).getTierUnit();
        final List<RawUsage> rawUsages = rawUsageDao.getRawUsage(trackingIds,tierUnit);
        int index=0;
        for(final TierDetail tierDetail: tierDetails.getTierDetails()){
            BigDecimal quantity = tierDetail.getQuantity();
            final BigDecimal tierPrice = tierDetail.getTierPrice();
            final BigDecimal tierBlockSize = tierDetail.getTierBlockSize();
            while (quantity.compareTo(BigDecimal.ZERO)>0){
                final RawUsage rawUsage = rawUsages.get(index);
                final BigDecimal amount = rawUsage.getAmount();
                rawUsage.setTier(tierDetail.getTier());
                if(amount.compareTo(quantity) <= 0){
                    quantity = quantity.subtract(amount);
                    final BigDecimal charges = Optional.ofNullable(rawUsage.getCharges()).orElse(BigDecimal.ZERO).add(
                            amount.multiply(tierPrice).divide(tierBlockSize));
                    rawUsage.setCharges(charges);
                    index++;
                } else {
                    rawUsage.setAmount(amount.subtract(quantity));
                    final BigDecimal charges = Optional.ofNullable(rawUsage.getCharges()).orElse(BigDecimal.ZERO).add(
                            quantity.multiply(tierPrice).divide(tierBlockSize));
                    rawUsage.setCharges(charges);
                    quantity = BigDecimal.ZERO;
                }
            }
        }
        publishCharges(rawUsages,tenant);
    }

    private void publishCharges(final Iterable<RawUsage> rawUsages, final String tenant) {
        rawUsageDao.persistRawUsageCharges(rawUsages);
    }

    public void invoice() {
        final List<RawUsage> uniqueSubsId = rawUsageDao.getUnbilledAggregationIds();
        for (final RawUsage rawUsage:uniqueSubsId){
            try {
                final CallContext context=getCallContext(rawUsage.getTenantId());
                final Subscription sub = killbillAPI.getSubscriptionApi().getSubscriptionForExternalKey(rawUsage.getSubscriptionId(), false, context);
                final Invoice invoice=killbillAPI.getInvoiceUserApi().triggerInvoiceGeneration(sub.getAccountId(), getTargetDate(rawUsage), List.of(), context);
                log.info("InvoiceTenant {}",invoice);
            } catch (final Exception e) {
                log.error("Invoice Error :: {} :: {}", e.getClass(), e.getMessage());
            }
        }
    }

    private LocalDate getTargetDate(final RawUsage rawUsage) {
        return rawUsageDao.getUsageSum(rawUsage).getMaxRecordDate().plusMonths(1).toLocalDate();
    }

    public void chargeUsage() {
        List<Long> performance = new ArrayList<>();
        long start = System.nanoTime();
        try {
            aggregateUsages();
        }catch (final Exception e){
            log.error("chargeUsage :: aggregateUsages :: {}",e.getMessage());
        }
        performance.add(System.nanoTime()-start);
        start = System.nanoTime();
        try {
            bill();
        }catch (final Exception e){
            log.error("chargeUsage :: bill :: {}",e.getMessage());
        }
        performance.add(System.nanoTime()-start);
        start = System.nanoTime();
        try {
            invoice();
        }catch (final Exception e){
            log.error("chargeUsage :: invoice :: {}",e.getMessage());
        }
        performance.add(System.nanoTime()-start);
        start = System.nanoTime();
        try {
            backFill();
        }catch (final Exception e){
            log.error("chargeUsage :: backFill :: {}",e.getMessage());
        }
        performance.add(System.nanoTime()-start);
        log.info("Performance :: {}",performance);
    }

    public ChargeDetails getCharges(final String tenantId, final String subscriptionId, final String unit, final String trackingId) {
        final RawUsage rawUsage= rawUsageDao.getRawUsage(tenantId,subscriptionId,unit,trackingId);
        if(Objects.isNull(rawUsage.getCharges())){
            throw new RequestTooEarly("Transaction Not Charged Yet");
        }
        final BigDecimal charge = Optional.ofNullable(rawUsage.getCharges()).orElse(BigDecimal.ZERO);
        return new ChargeDetails().setCharges(charge).setTax(charge.multiply(BigDecimal.valueOf(TAX_RATE)));
    }

    public ChargeDetails getChargesForPG(final String tenantId, final String subscriptionId, final String unit, final String trackingId) {
        final RawUsage rawUsage= rawUsageDao.getRawUsage(tenantId,List.of(subscriptionId.concat("_VOLUME"),subscriptionId.concat("_COUNT")),unit,trackingId);
        if(Objects.isNull(rawUsage.getCharges())){
            throw new RequestTooEarly("Transaction Not Charged Yet");
        }
        final BigDecimal charge = Optional.ofNullable(rawUsage.getCharges()).orElse(BigDecimal.ZERO);
        return new ChargeDetails().setCharges(charge).setTax(charge.multiply(BigDecimal.valueOf(TAX_RATE)));
    }
}
