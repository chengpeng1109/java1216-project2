package com.alibaba.cainiao.cndcp.web.module.action;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.Resource;
import javax.servlet.http.HttpSession;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeanUtils;
import com.alibaba.cainiao.cm.permission.tools.PermissionUtil;
import com.alibaba.cainiao.cndcp.app.eventdomain.constants.LogisticsScheduleConstants;
import com.alibaba.cainiao.cndcp.app.eventdomain.fee.TradeTaxCodeClient;
import com.alibaba.cainiao.cndcp.app.eventdomain.handler.Result;
import com.alibaba.cainiao.cndcp.app.eventdomain.template.EventType;
import com.alibaba.cainiao.cndcp.core.tmsorder.dataobject.TmsOrderDO;
import com.alibaba.cainiao.cndcp.core.tmsorder.service.TmsOrderService;
import com.alibaba.cainiao.cndcp.core.tmsorder.util.TmsOrderUtils;
import com.alibaba.cainiao.cndcp.core.utils.TmsOrderStatusUtil;
import com.alibaba.cainiao.cndcp.core.utils.ToolSendResult;
import com.alibaba.cainiao.cndcp.hsfclient.lc.LogisticsorderCenterClient;
import com.alibaba.cainiao.cndcp.hsfclient.wlbaccount.WlbAccountClient;
import com.alibaba.cainiao.cndcp.notifysender.DcpTmsOrderNotifySender;
import com.alibaba.cainiao.cndcp.order.constants.AccountSendEnumAction;
import com.alibaba.cainiao.cndcp.order.dto.DcpTmsOrderDTO;
import com.alibaba.cainiao.cndcp.order.dto.ResultDTO;
import com.alibaba.citrus.turbine.Context;
import com.alibaba.citrus.turbine.Navigator;
import com.alibaba.citrus.turbine.dataresolver.Param;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.lp.share.web.user.session.SessionHelper;
import com.alibaba.lp.share.web.user.session.SessionUser;
import com.google.common.base.Preconditions;
import com.taobao.hsf.notify.client.SendResult;
import com.taobao.logistics.constants.LogisticsOrderConstant;
import com.taobao.logistics.domain.dataobject.OrderGoodsDO;
import com.taobao.logistics.domain.query.LogisticsOrderQueryOptionTO;
import com.taobao.logistics.domain.result.LogisticsOrderQueryResultTO;
import com.taobao.logistics.schedulecenter.client.notify.ScheduleNotifyPublisher;
import com.taobao.logistics.schedulecenter.constants.BizAppConstant;
import com.taobao.logistics.schedulecenter.domain.AppInfo;
import com.taobao.logistics.schedulecenter.domain.dto.ScheduleTaskDTO;
import com.taobao.wlb.accountprod.client.dto.TinItemDTO;
import com.taobao.wlb.res.client.constants.ResConstants;

/**
 * @author wb-chengpeng.b E-mail: wb-chengpeng.b@alibaba-inc.com
 * @date 创建时间：2015-6-4 上午10:44:02
 * @version 1.0
 * @parameter
 * @since
 * @return
 */
public class AccountSendAction {

	@Resource
	private TmsOrderService tmsOrderService;
	@Resource
	private LogisticsorderCenterClient logisticsorderCenterClient;
	@Resource
	private TradeTaxCodeClient tradeTaxCodeClient;
	@Resource
	private WlbAccountClient wlbAccountClient;
	@Resource
	private ScheduleNotifyPublisher scheduleNotifyPublisher;
	@Resource
	private DcpTmsOrderNotifySender dcpTmsOrderNotifySender;
	@Resource
	private PermissionUtil permissionUtilSpecial;

	private final static Logger logger = LoggerFactory.getLogger(AccountSendAction.class);

	private static final String MSG = "errorMsg";
	private static final String TIN_SERVICE_ITEM_ID = "5000000000004";// 一般进口
	private static final String BIM_SERVICE_ITEM_ID = "5000000000009";// 保税进口
	private static final String HTB_SERVICE_ITEM_ID = "5000000000018";//海淘宝
	private static final int MAX_LP_NUM = 200;
	private final static String spiltStr = "|";
	private static final String TAB_INDEX = "tabIndex";
	private static final String REGEX = "[a-zA-Z0-9 ,]+";
	private static final String REGEXFIT="\\s*|\t|\r|\n";
	
	public void doAccountSend(@Param("eventType") String eventType, @Param("lgOrderCode") String lgOrderCode,
			@Param("importType") String importType, @Param("nodeType") String nodeType, Context context,
			HttpSession session, Navigator navigator) {
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		String strDate = format.format(new Date());
		context.put(TAB_INDEX, 2);
		context.put("lgOrderCodes", lgOrderCode);
		SessionUser sessionUser = SessionHelper.getSessionUser(session);
        //2个值本来是想保存select选中状态
		context.put("selectvalue1",eventType);
		context.put("selectvalue2",importType);
		context.put("selectvalue3",nodeType);
		if (!permissionUtilSpecial.isGranted("/accountSend.htm")) {		
			context.put(MSG, "对不起您没有权限操作！");
			return;
		}
		if (StringUtils.isBlank(eventType) || StringUtils.isBlank(lgOrderCode)) {
			context.put(MSG, "下发指令类型或LP号不能为空！");
			return;
		}
		if (!StringUtils.isBlank(lgOrderCode)) {
            Pattern p = Pattern.compile(REGEXFIT);
            Matcher m = p.matcher(lgOrderCode);
            lgOrderCode = m.replaceAll("");
        }

		if (!lgOrderCode.matches(REGEX)) {
			context.put(MSG, "输入内容只能包含字母、数字或英文状态下','");
			return;
		}
		if (EventType.CP_PAY_TMS_AND_CUSTOMS_FEE.equals(eventType)) {
			if (StringUtils.isBlank(importType) || StringUtils.isBlank(nodeType)) {
				context.put(MSG, "CP结算支付需要选择完整下发类型！");
				return;
			}
		}
		ToolSendResult toolSendResult = new ToolSendResult();
		String[] orderCodes = StringUtils.split(lgOrderCode, ',');
		toolSendResult.setReceviedCount(orderCodes.length);
		StringBuffer msgId= new StringBuffer("");;
		if (orderCodes.length > MAX_LP_NUM) {
			context.put(MSG, "LP号数量不大于200个！");
			return;
		}
		Set<String> sendedSet = new HashSet<String>(); 	//避免重复下发
		Map<String, String> mapping = new LinkedHashMap<String, String>();
		mapping.put("importTypeTin", "一般进口");
		mapping.put("importTypeBim", "保税进口");
		mapping.put("importTypeHtb", "海淘宝");
		mapping.put("5000000000004", "一般进口");
		mapping.put("5000000000009", "保税进口");
		mapping.put("5000000000018", "海淘宝");
		for (String orderCode : orderCodes) {
			orderCode = StringUtils.trim(orderCode);
			// 检查订单号码是一般进口/保税/海淘宝
			String service_ItemCode = check_OrderCode(orderCode);

			if (service_ItemCode.equals(orderCode)) {
				toolSendResult.getTmsOrderNotFoundCode().add(orderCode);
				continue;
			}
			if (sendedSet.contains(orderCode)) {
				continue;
			} else {
				sendedSet.add(orderCode);
			}
			
			if(EventType.CP_PAY_TMS_AND_CUSTOMS_FEE.equals(eventType)){
				if(service_ItemCode.equals(TIN_SERVICE_ITEM_ID) && !importType.equals("importTypeTin")||
				   service_ItemCode.equals(BIM_SERVICE_ITEM_ID) && !importType.equals("importTypeBim")||
			       service_ItemCode.equals(HTB_SERVICE_ITEM_ID) && !importType.equals("importTypeHtb")){
					toolSendResult.getStatusInvalidCode().put(orderCode,"该运单是"+mapping.get(service_ItemCode)+"运单号，不能在"+mapping.get(importType)+"下进行结算！");
					continue;
				}

			}
			
			
			Result result = null;
			try {
				if (EventType.CONFIRM_PAY_TMS_AND_CUSTOMS_FEE.equals(eventType)) {
					if (service_ItemCode.equals(TIN_SERVICE_ITEM_ID)) {
						result = payCustomsFee(orderCode);// 从此调用组装Notify消息体
						logger.info(spiltStr+strDate + spiltStr + sessionUser.getLoginId() + spiltStr
								+ orderCode + spiltStr + "AccountSendAction" + spiltStr + result.isSuccess()+","+eventType+","+importType+","+nodeType);// TLog记录格式
					} else if (service_ItemCode.equals(BIM_SERVICE_ITEM_ID)) {
						result = payFee(orderCode,msgId);
						logger.info(spiltStr+strDate + spiltStr + sessionUser.getLoginId() + spiltStr
								+ orderCode + spiltStr + "AccountSendAction" + spiltStr + result.isSuccess()+","+eventType+","+importType+","+nodeType);
					}else{
						toolSendResult.getFailCode().put(orderCode, "不是一般|保税运单号无法进行商家结算支付!");
						continue;
					}
				} else if (EventType.CP_PAY_TMS_AND_CUSTOMS_FEE.equals(eventType)) {
					TmsOrderDO tmsOrderDO = checkOrderCode(orderCode, nodeType);
					if (tmsOrderDO == null) {
						toolSendResult.getFailCode().put(orderCode, "根据下发指令没有查询到相关子配送单信息!");
						continue;
					}
					
					if(!TmsOrderStatusUtil.statusValidateAccount(tmsOrderDO.getServiceItemId().toString(),tmsOrderDO.getStatus(),nodeType)) {//如果根据状态值没有匹配信息则无法下发
						toolSendResult.getStatusInvalidCode().put(tmsOrderDO.getLgOrderCode(),
								"当前状态'"+TmsOrderStatusUtil.getDescription(tmsOrderDO.getServiceItemId().toString(), tmsOrderDO.getStatus())+"'不满足结算状态!");
						continue;
					}
					result = cpPayFee(tmsOrderDO,msgId);// CP结算重推
					logger.info(spiltStr+strDate + spiltStr + sessionUser.getLoginId() + spiltStr
							+ orderCode + spiltStr + "AccountSendAction" + spiltStr + result.isSuccess()+","+eventType+","+importType+","+nodeType);
				} else {
					context.put(MSG, "不可知的指令类型！");
					return;
				}
			} catch (Exception e) {
				context.put(MSG, "程序异常请联系技术支持!"+e.getMessage());
				return;
			}

			if (result.isSuccess()) {
				//context.put(MSG, "指令下发成功！msgId="+msgId);
				if(EventType.CONFIRM_PAY_TMS_AND_CUSTOMS_FEE.equals(eventType)){
					toolSendResult.getSuccessCode().put(orderCode, "下发成功");
				}else{
					toolSendResult.getSuccessCode().put(orderCode, "下发成功,msgId="+msgId);
				}
	
				toolSendResult.addSuccessCount();
			} else {
				toolSendResult.getFailCode().put(orderCode, result.getErrorMsg());
			}

		}
		context.put("toolSendResult", toolSendResult);
		context.put("failCount", toolSendResult.getFailCount());
	}

	// 支付运费一般进口
	private Result payCustomsFee(String lgOrderCode) {
		// 检查物流订单
		LogisticsOrderQueryOptionTO option = new LogisticsOrderQueryOptionTO();
		option.setShowOrderGoods(Boolean.TRUE);
		LogisticsOrderQueryResultTO logisticsOrderQueryResultTO = logisticsorderCenterClient.queryLogisticsOrderByCode(
				lgOrderCode, option);
		if (!logisticsOrderQueryResultTO.isSuccess() || logisticsOrderQueryResultTO.getOrder() == null) {
			return Result.LGORDER_NOT_EXIST;
		}
		// 调用支付接口,开始支付
		List<OrderGoodsDO> ordergoodslist = logisticsOrderQueryResultTO.getGoodsList();
		if (ordergoodslist == null || ordergoodslist.isEmpty()) {
			return Result.PROCESS_EXCEPTION_PLEASE_RETRY;
		}
		Iterator<OrderGoodsDO> iterator = ordergoodslist.iterator();
		final List<TinItemDTO> itemList = new ArrayList<TinItemDTO>();
		while (iterator.hasNext()) {
			TinItemDTO tinItem = new TinItemDTO();
			OrderGoodsDO ordergoods = iterator.next();
			String taxCode = tradeTaxCodeClient.getTaxCodeByTradeId(ordergoods.getTradeId());
			if (taxCode == null) {
				return Result.TIN_TAX_CODE_UNFOUND;
			}
			tinItem.setPostTaxNumber(taxCode);
			tinItem.setQuantity(ordergoods.getGoodsQuantity().intValue());
			tinItem.setPrice(Long.valueOf(ordergoods
					.getFeature(LogisticsOrderConstant.ORDER_GOODS_FEATURE_NAME_ACTUAL_VALUE)));
			tinItem.setItemId(Long.valueOf(ordergoods.getFeature(LogisticsOrderConstant.FEATURE_ITEM_ID)));
			itemList.add(tinItem);
		}
		// 调用支付，幂等
		ResultDTO<Void> payResult = wlbAccountClient.payTmsAndCustomsFee(lgOrderCode, itemList);
		if (payResult == null) {
			return Result.TIN_PAY_TMS_CUSTOMS_FEE_FAILURE;
		}
		if (!payResult.isSuccess()) {
			return new Result(payResult.getErrorCode(), payResult.getErrorMsg(), false, null);
		}
		return Result.getSuccessReslut();
	}

	// 支付运费保税进口
	public Result payFee(String lgOrderCode,StringBuffer msgId) {
		TmsOrderDO parentTmsOrder = tmsOrderService.findParentTmsOrder(lgOrderCode);
		if (parentTmsOrder == null) {
			return Result.TMS_ORDER_NOT_FOUND;
		}
		// 创建 ScheduleTaskDTO
		ScheduleTaskDTO scheduleTaskDTO = new ScheduleTaskDTO();
		AppInfo appInfo = AppInfo.newInstance(BizAppConstant.CNDCP, LogisticsScheduleConstants.TaskType.BIM);
		scheduleTaskDTO.setAppInfo(appInfo);
		scheduleTaskDTO.setEntityId(parentTmsOrder.getLgOrderId()); // lg_order的id
		scheduleTaskDTO.setTaskType(LogisticsScheduleConstants.TaskType.BIM);
		scheduleTaskDTO.addParam(LogisticsScheduleConstants.TaskParam.LG_ORDER_CODE, lgOrderCode);
		scheduleTaskDTO.addParam(LogisticsScheduleConstants.TaskParam.LBX_CODE, lgOrderCode);
		scheduleTaskDTO.addParam(LogisticsScheduleConstants.TaskParam.INNNER_TASK_TYPE,
				LogisticsScheduleConstants.InnerTaskType.PAY_TMS_AND_CUSTOMS_FEE);
		SendResult sendResult = scheduleNotifyPublisher.publishNotify(scheduleTaskDTO);

		Result result = new Result();
		if (!sendResult.isSuccess()) {
			result.setErrorMsg("支付申请失败,发送调度中心失败,msgId:" + sendResult.getMessageId() + ",msg:"
					+ sendResult.getErrorMessage());
			result.setSuccess(false);
		}
         msgId.append(sendResult.getMessageId());
		return result;
	}

	public Result cpPayFee(TmsOrderDO tmsOrderDO, StringBuffer msgId) {
		Result result = new Result();
		DcpTmsOrderDTO dcpTmsOrderDTO = TmsOrderUtils.transformToDTO(tmsOrderDO);
		SendResult sendResult = dcpTmsOrderNotifySender.sendNoTransactionMessage(dcpTmsOrderDTO);
		if (!sendResult.isSuccess()) {
			result.setErrorMsg("CP支付申请失败,发送调度中心失败,msgId:" + sendResult.getMessageId() + ",msg:"
					+ sendResult.getErrorMessage());
			result.setSuccess(false);
		}
		msgId.append(sendResult.getMessageId());
		return result;
	}

	public static DcpTmsOrderDTO transformToDTO(TmsOrderDO tmsOrderDO) {
		Preconditions.checkNotNull(tmsOrderDO);

		DcpTmsOrderDTO dcpTmsOrderDTO = new DcpTmsOrderDTO();
		try {
			BeanUtils.copyProperties(tmsOrderDO, dcpTmsOrderDTO);
			String feature = tmsOrderDO.getFeature();
			if (org.springframework.util.StringUtils.hasText(feature)) {
				@SuppressWarnings("unchecked")
				Map<String, String> featureMap = (Map<String, String>) JSONObject.parseObject(feature, Map.class);
				if (featureMap != null) {
					dcpTmsOrderDTO.setFeatureMap(featureMap);
				}
			}
		} catch (Exception e) {
			logger.error("属性复制失败！", e);
			return null;
		}

		return dcpTmsOrderDTO;
	}

	// 检查订单号是否存在
	private String check_OrderCode(String orderCode) {
		// 一般进口
		TmsOrderDO tmsOrderDOTin = tmsOrderService.queryOne(orderCode,
				String.valueOf(ResConstants.ResType.Tran_Store.getCatId()));
		// 保税进口
		TmsOrderDO tmsOrderDOBim = tmsOrderService.queryOne(orderCode,
				String.valueOf(ResConstants.ResType.CUSTOMS.getCatId()));

		if (tmsOrderDOTin == null && tmsOrderDOBim == null)
			return orderCode;
		

		if (tmsOrderDOTin != null && TIN_SERVICE_ITEM_ID.equals(tmsOrderDOTin.getServiceItemId().toString()))
			return TIN_SERVICE_ITEM_ID;
		//海淘宝和一般进口模式一样，且service_tiem_id 不同
	    if(tmsOrderDOTin != null && HTB_SERVICE_ITEM_ID.equals(tmsOrderDOTin.getServiceItemId().toString()))
			return HTB_SERVICE_ITEM_ID;
			

		if (tmsOrderDOBim != null && BIM_SERVICE_ITEM_ID.equals(tmsOrderDOBim.getServiceItemId().toString()))
			return BIM_SERVICE_ITEM_ID;

		return orderCode;
	}

	/**
	 * CP支付检查LP运单号，不区分一般，保税，
	 * 根据nodeType来判断 nodeType支付节点 
	 * Tran_Store出站 standard_status=12 
	 * TRUNK干线 standard_status=15
	 * GATE报关中 standard_status=20 ,standard_status =30 海关查没
	 *  DISTRIBUTOR国内配送 standard_status=25 
	 *  CUSTOMS海关
	 */
	private TmsOrderDO checkOrderCode(String orderCode,String nodeType) {

		TmsOrderDO tmsOrderDO = null;
		if (ResConstants.ResType.Tran_Store.name().equals(nodeType)) {
			tmsOrderDO = tmsOrderService
					.queryOne(orderCode, String.valueOf(ResConstants.ResType.Tran_Store.getCatId()));

		} else if (ResConstants.ResType.TRUNK.name().equals(nodeType)) {
			tmsOrderDO = tmsOrderService.queryOne(orderCode, String.valueOf(ResConstants.ResType.TRUNK.getCatId()));

		} else if ( AccountSendEnumAction.AccountSendEnum.GATE_CONFISCATE.name().equals(nodeType) || AccountSendEnumAction.AccountSendEnum.GATE_CONFISCATE_BIM.name().equals(nodeType)) {
			tmsOrderDO = tmsOrderService.queryOne(orderCode, String.valueOf(ResConstants.ResType.GATE.getCatId()));
		} else if (ResConstants.ResType.DISTRIBUTOR.name().equals(nodeType) || AccountSendEnumAction.AccountSendEnum.DISTRIBUTOR_BIM.name().equals(nodeType)) {
			tmsOrderDO = tmsOrderService.queryOne(orderCode,String.valueOf(ResConstants.ResType.DISTRIBUTOR.getCatId()));

		} else if (ResConstants.ResType.CUSTOMS.name().equals(nodeType) || AccountSendEnumAction.AccountSendEnum.GATE_DECLARE.name().equals(nodeType)) {
			tmsOrderDO = tmsOrderService.queryOne(orderCode, String.valueOf(ResConstants.ResType.CUSTOMS.getCatId()));

		}else {
			logger.error("未知的请求，orderCode=" + orderCode + ",nodeType=" + nodeType);
			return null;
		}
		if (tmsOrderDO == null) {
			logger.error(nodeType + "子配送单信息不存在,orderCode=" + orderCode);
			return null;
		}

		

		return tmsOrderDO;
	}
	

	
	
	/*private boolean checkTmsOrderisType(TmsOrderDO tmsOrderDO,String nodetype){
		boolean b=false;

		if(TmsOrderStatusUtil.statusValidateAccount(tmsOrderDO.getServiceItemId().toString(),tmsOrderDO.getStatus()))
			b = true;	
	if(AccountSendEnumAction.AccountSendEnum.GATE_CONFISCATE_BIM.name().equals(nodetype) && AccountSendEnumAction.AccountSendEnum.getKeyByDesc(nodetype).equals(tmsOrderDO.getStandardStatus()) )
			b = true;
		if(AccountSendEnumAction.AccountSendEnum.GATE_DECLARE.name().equals(nodetype) && AccountSendEnumAction.AccountSendEnum.getKeyByDesc(nodetype).equals(tmsOrderDO.getStandardStatus()) )
			b = true;
		if(AccountSendEnumAction.AccountSendEnum.GATE_TAX.name().equals(nodetype) && AccountSendEnumAction.AccountSendEnum.getKeyByDesc(nodetype).equals(tmsOrderDO.getStandardStatus()) )
			b = true;

		return b;
	}*/
	
}
