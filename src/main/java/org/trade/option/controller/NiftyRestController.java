package org.trade.option.controller;

import org.springframework.data.domain.Sort;
import org.springframework.web.bind.annotation.*;
import org.trade.option.client.grow.OcSymbolEnum;
import org.trade.option.entity.Nifty;
import org.trade.option.entity.SpotPrice;
import org.trade.option.service.iface.NiftyService;
import org.trade.option.service.iface.SpotPriceService;
import org.trade.option.utils.ExpiryUtils;
import org.trade.option.utils.OptionTypeEnum;

import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

@RestController
@RequestMapping("nifty")
public class NiftyRestController {
    private final SpotPriceService spotPriceService;
    private final NiftyService niftyService;
    private static final Integer noOfStrikesPricesInEachCompartment = 3;
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd-MMM-yyyy");

    public NiftyRestController(SpotPriceService spotPriceService, NiftyService niftyService) {
        this.spotPriceService = spotPriceService;
        this.niftyService = niftyService;
    }

    @GetMapping(value = {"refresh/{sortBy}/{orderBy}"})
    public Map<String, Object> refresh(@PathVariable(required = false) String sortBy, @PathVariable(required = false) String orderBy) {
        Map<String, Object> response = new HashMap<>();
        SpotPrice niftySpot = spotPriceService.getLastInserted(OcSymbolEnum.NIFTY.getOhlcSymbol());
        String inputDay = LocalDate.now(ZoneId.of("Asia/Kolkata")).format(formatter);
        Sort sort = Sort.by(sortBy);
        if(orderBy != null)
            sort = orderBy.equals("ASC") ? sort.ascending() : sort.descending();
        List<Nifty> niftyList = niftyService.findAll(inputDay, sort);
        List<Nifty> niftyCeList = filter(niftyList, OptionTypeEnum.CE.name());
        List<Nifty> niftyPeList = filter(niftyList, OptionTypeEnum.PE.name());
        Double bankNiftySpotPrice = niftySpot.getLastPrice();
        response.put("niftyCeList", niftyCeList);
        response.put("niftyPeList", niftyPeList);
        response.put("niftySpotPrice", bankNiftySpotPrice);
        return response;
    }

    @GetMapping(value = {"refreshAnalysis/{sortBy}/{orderBy}"})
    public Map<String, Object> refreshAnalysis(@PathVariable(required = false) String sortBy, @PathVariable(required = false) String orderBy) {
        SpotPrice niftySpot = spotPriceService.getLastInserted(OcSymbolEnum.NIFTY.getOhlcSymbol());
        Integer atmStrike = ExpiryUtils.getATM(niftySpot.getLastPrice());
        String inputDay = LocalDate.now(ZoneId.of("Asia/Kolkata")).format(formatter);
        Sort sort = Sort.by(sortBy);
        if(orderBy != null)
            sort = orderBy.equals("ASC") ? sort.ascending() : sort.descending();
        List<String> insertedTimeList = niftyService.getInsertedTimeList(inputDay, sort);
        List<Nifty> todayData = niftyService.findByUdatedAtSource(inputDay, sort);
        List<Map<Integer, List<Nifty>>> segment1 = new ArrayList<>();
        Map<String, Object> response = new HashMap<>();
        response.put("compartment1", prepareCompartment1(atmStrike, insertedTimeList, todayData, inputDay));
        response.put("compartment2", prepareCompartment2(atmStrike, insertedTimeList, todayData, inputDay));
        response.put("compartment3", prepareCompartment3(atmStrike, insertedTimeList, todayData, inputDay));
        response.put("compartment4", prepareCompartment4(atmStrike, insertedTimeList, todayData, inputDay));
        response.put("insertedTimeList", insertedTimeList);
        response.put("niftyATM", atmStrike);
        response.put("niftySpot", niftySpot.getUpdatedAtSource().replace(inputDay, "").trim() +": "+ niftySpot.getLastPrice());
        response.put("currentDate", inputDay);
        return response;
    }

    @GetMapping(value = {"/refreshIndexes"})
    public @ResponseBody Map<String, Object> refreshAnalysis() {
        String inputDay = LocalDate.now(ZoneId.of("Asia/Kolkata")).format(formatter);
        Map<String, Object> response = new HashMap<>();
        response.put("niftyToday", spotPriceService.getSpotPriceBySymbol(OcSymbolEnum.NIFTY.getOhlcSymbol(), inputDay, Sort.by("id").ascending()));
        response.put("bankNiftyToday", spotPriceService.getSpotPriceBySymbol(OcSymbolEnum.BANK_NIFTY.getOhlcSymbol(), inputDay, Sort.by("id").ascending()));

        return response;
    }
    private Map<Integer, Map> prepareCompartment1(Integer atmStrike, List<String> insertedTimeList, List<Nifty> todayData, String inputDay) {
        Integer depth = 50;
        Integer maxStrikePrice = atmStrike + (noOfStrikesPricesInEachCompartment * depth);
        Integer minStrikePrice = atmStrike - (noOfStrikesPricesInEachCompartment * depth);

        Map<Integer, List<Nifty>> strikeWiseData = todayData.stream()
                .filter(nf -> nf.getStrikePrice() <= atmStrike && nf.getStrikePrice() > (minStrikePrice-50) && nf.getOptionType().equals("CE"))
                .collect(Collectors.groupingBy(Nifty::getStrikePrice));
        Map<Integer, Map> compartment1 = new HashMap<>();
        for(Integer strikePrice: strikeWiseData.keySet()) {
            Map<String, Long> insertedTimeItOi = new HashMap<>();
            for(String insertedTime : insertedTimeList) {
                insertedTimeItOi.put(insertedTime, getDataAtInsertedTime(strikeWiseData.get(strikePrice), insertedTime, inputDay));
            }
            Map<String, Long> result = insertedTimeItOi.entrySet().stream()
                    .sorted(Map.Entry.comparingByKey())
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue,
                            (oldValue, newValue) -> oldValue, LinkedHashMap::new));

            compartment1.put(strikePrice, result);
        }
        return compartment1;
    }

    private Map<Integer, Map> prepareCompartment2(Integer atmStrike, List<String> insertedTimeList, List<Nifty> todayData, String inputDay) {
        Integer depth = 50;
        Integer maxStrikePrice = atmStrike + (noOfStrikesPricesInEachCompartment * depth);
        Integer minStrikePrice = atmStrike - (noOfStrikesPricesInEachCompartment * depth);
        Map<Integer, List<Nifty>> strikeWiseData = todayData.stream()
                .filter(nf -> nf.getStrikePrice() > atmStrike && nf.getStrikePrice() <= maxStrikePrice && nf.getOptionType().equals("CE"))
                .collect(Collectors.groupingBy(Nifty::getStrikePrice));
        Map<Integer, Map> compartment2 = new HashMap<>();
        for(Integer strikePrice: strikeWiseData.keySet()) {
            Map<String, Long> insertedTimeItOi = new HashMap<>();
            for(String insertedTime : insertedTimeList) {
                insertedTimeItOi.put(insertedTime, getDataAtInsertedTime(strikeWiseData.get(strikePrice), insertedTime, inputDay));
            }
            Map<String, Long> result = insertedTimeItOi.entrySet().stream()
                    .sorted(Map.Entry.comparingByKey())
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue,
                            (oldValue, newValue) -> oldValue, LinkedHashMap::new));

            compartment2.put(strikePrice, result);
        }
        return compartment2;
    }

    private Map<Integer, Map> prepareCompartment4(Integer atmStrike, List<String> insertedTimeList, List<Nifty> todayData, String inputDay) {
        Integer depth = 50;
        Integer maxStrikePrice = atmStrike + (noOfStrikesPricesInEachCompartment * depth);
        Integer minStrikePrice = atmStrike - (noOfStrikesPricesInEachCompartment * depth);
        Map<Integer, List<Nifty>> strikeWiseData = todayData.stream()
                .filter(nf -> nf.getStrikePrice() > atmStrike && nf.getStrikePrice() <= maxStrikePrice && nf.getOptionType().equals("PE"))
                .collect(Collectors.groupingBy(Nifty::getStrikePrice));
        Map<Integer, Map> compartment3 = new HashMap<>();
        for(Integer strikePrice: strikeWiseData.keySet()) {
            Map<String, Long> insertedTimeItOi = new HashMap<>();
            for(String insertedTime : insertedTimeList) {
                insertedTimeItOi.put(insertedTime, getDataAtInsertedTime(strikeWiseData.get(strikePrice), insertedTime, inputDay));
            }
            Map<String, Long> result = insertedTimeItOi.entrySet().stream()
                    .sorted(Map.Entry.comparingByKey())
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue,
                            (oldValue, newValue) -> oldValue, LinkedHashMap::new));

            compartment3.put(strikePrice, result);
        }
        return compartment3;
    }
    private Map<Integer, Map> prepareCompartment3(Integer atmStrike, List<String> insertedTimeList, List<Nifty> todayData, String inputDay) {
        Integer depth = 50;
        Integer maxStrikePrice = atmStrike + (noOfStrikesPricesInEachCompartment * depth);
        Integer minStrikePrice = atmStrike - (noOfStrikesPricesInEachCompartment * depth);
        Map<Integer, List<Nifty>> strikeWiseData = todayData.stream()
                .filter(nf -> nf.getStrikePrice() <= atmStrike && nf.getStrikePrice() > (minStrikePrice-50) && nf.getOptionType().equals("PE"))
                .collect(Collectors.groupingBy(Nifty::getStrikePrice));
        Map<Integer, Map> compartment4 = new HashMap<>();
        for(Integer strikePrice: strikeWiseData.keySet()) {
            Map<String, Long> insertedTimeItOi = new HashMap<>();
            for(String insertedTime : insertedTimeList) {
                insertedTimeItOi.put(insertedTime, getDataAtInsertedTime(strikeWiseData.get(strikePrice), insertedTime, inputDay));
            }
            Map<String, Long> result = insertedTimeItOi.entrySet().stream()
                    .sorted(Map.Entry.comparingByKey())
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue,
                            (oldValue, newValue) -> oldValue, LinkedHashMap::new));

            compartment4.put(strikePrice, result);
        }
        return compartment4;
    }



    private Long getDataAtInsertedTime(List<Nifty> niftyList, String insertedTime, String inputDay) {
        Nifty nifty = niftyList.stream().filter(n -> n.getUpdatedAtSource().replace(inputDay,"").equals(insertedTime)).findFirst().orElse(null);
        return nifty != null ? nifty.getChangeInOi() : 0;
    }

    private List<Nifty> filter(List<Nifty> optionDataList, String optionType) {
        return optionDataList.stream()
                .filter(n -> n.getOptionType().equals(optionType))
                .sorted(Comparator.comparing(Nifty::getId).reversed())
                .collect(Collectors.toList());
    }
}
