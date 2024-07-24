package org.killbill.billing.plugin.meter.domain.external;

import java.util.List;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class ConsumerUnitUsageRecord {
    private String unitType;
    private List<ConsumerUsageRecord> usageRecords;
}
