using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data.Entity;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using xxx.xxx.Domain.Abstract;
using xxx.xxx.Domain.Models;
using xxx.xxx.Domain.Models.DB;
using xxx.xxx.Services.Logging;

namespace xxx.xxx.Domain.Logics
{
    public interface IOrdersWatcherManager
    {
        void StartOrderTracking(OpenOrderModel order);
        void StopOrderTracking(OpenOrderModel orderIdentifier);

        IDictionary<string, List<OpenOrderModel>> GetAllOrders();
        IList<OpenOrderModel> GetAllOrdersForExchange(string exchange);

        event EventHandler<OpenOrderStatusUpdateArgs> OnOrderStatusUpdated;
    }

    public class OrdersWatcherManager : IOrdersWatcherManager
    {
        private readonly IOrdersWatcher _ordersWatcher;
        private readonly IInstrumentsNameMapper _instrumentsNameMapper;
        private readonly Func<DbContext> _contextCreator;
        private readonly ILogger _logger;
        private readonly IOrderCancelationWatcher _orderCancelationWatcher;
        private readonly IDictionaryModelsMapper _dictionaryModelsMapper;

        private readonly ConcurrentDictionary<string, OpenOrderModel> _allOrders;

        public OrdersWatcherManager(IOrdersWatcher ordersWatcher, IInstrumentsNameMapper instrumentsNameMapper,
            Func<DbContext> contextCreator, IOrderCancelationWatcher orderCancelationWatcher, ILogger logger,
            IDictionaryModelsMapper dictionaryModelsMapper)
        {
            _ordersWatcher = ordersWatcher;
            _instrumentsNameMapper = instrumentsNameMapper;
            _contextCreator = contextCreator;
            _orderCancelationWatcher = orderCancelationWatcher;
            _dictionaryModelsMapper = dictionaryModelsMapper;
            _logger = logger ?? new DefaultLogger();

            _allOrders = new ConcurrentDictionary<string, OpenOrderModel>();

            _ordersWatcher.OnOrderStatusUpdated += (sender, args) =>
            {
                try
                {
                    var openOrderStatusUpdateArgs = new OpenOrderStatusUpdateArgs
                    {
                        OrderIdentifier = new OrderIdentifier(args.OrderIdentifier.Value, _instrumentsNameMapper.GetGeneralInstrument(args.OrderIdentifier.Instrument.Name, args.Exchange)),
                        OrderStatus = args.OrderStatus,
                        OpenOrderModel = _allOrders[args.OrderIdentifier.Value]
                    };
                    Volatile.Read(ref OnOrderStatusUpdated)?.Invoke(this, openOrderStatusUpdateArgs);
                }
                catch (Exception e)
                {
                    _logger?.Log(SeverityLevel.Error, $"Failed to start send update event for Order {args.OrderIdentifier}");
                    _logger?.Log(SeverityLevel.Error, e);
                }
            };
            Task.Run(async () => await StartTrackingAllOpenOrders()).Wait();
        }

        public event EventHandler<OpenOrderStatusUpdateArgs> OnOrderStatusUpdated;

        public void StartOrderTracking(OpenOrderModel order)
        {
            try
            {
                var exchange = _dictionaryModelsMapper.GetName<ExchangeModel>(order.ExchangeId);
                var targetInstrument = _instrumentsNameMapper.GetExchangeInstrument(
                    _dictionaryModelsMapper.GetName<InstrumentModel>(order.InstrumentId), exchange);

                var orderIdentifier = new OrderIdentifier(order.OrderId, targetInstrument);
                _ordersWatcher.StartOrderTracking(orderIdentifier, exchange);
                _allOrders.AddOrUpdate(order.OrderId, order, (s, model) => order);
                _orderCancelationWatcher.AddToWatcher(order);
            }
            catch (Exception e)
            {
                _logger?.Log(SeverityLevel.Error, $"Failed to start tracking for new order! Exchange: {order.ExchangeId}, OrderId = {order.OrderId}");
                _logger?.Log(SeverityLevel.Error, e);
                throw;
            }
        }

        public void StopOrderTracking(OpenOrderModel order)
        {
            try
            {
                var exchange = _dictionaryModelsMapper.GetName<ExchangeModel>(order.ExchangeId);
                var targetInstrument = _instrumentsNameMapper.GetExchangeInstrument(
                    _dictionaryModelsMapper.GetName<InstrumentModel>(order.InstrumentId), exchange);
                var orderIdentifier = new OrderIdentifier(order.OrderId, targetInstrument);

                _allOrders.TryRemove(order.OrderId, out OpenOrderModel removedOrder);
                _ordersWatcher.StopOrderTracking(orderIdentifier, exchange);
                _orderCancelationWatcher.RemoveFromWatcher(order);
            }
            catch (Exception e)
            {
                _logger?.Log(SeverityLevel.Error, $"Failed to start tracking for new order! Exchange: {order.ExchangeId}, OrderId = {order.OrderId}");
                _logger?.Log(SeverityLevel.Error, e);
                throw;
            }
        }

        public IDictionary<string, List<OpenOrderModel>> GetAllOrders()
        {
            return _allOrders.Values.GroupBy(x => x.ExchangeId).ToDictionary(x => _dictionaryModelsMapper.GetName<ExchangeModel>(x.Key), y => y.ToList());
        }

        public IList<OpenOrderModel> GetAllOrdersForExchange(string exchange)
        {
            var exchangeId = _dictionaryModelsMapper.GetId<ExchangeModel>(exchange);
            var orders = _allOrders.Values.Where(x => x.ExchangeId == exchangeId).ToList();
            return orders;
        }

        private async Task StartTrackingAllOpenOrders()
        {
            try
            {
                using (var db = _contextCreator())
                {
                    var allOpenedOrders = await db.Set<OpenOrderModel>()
                        .Where(x => x.OrderStatus == OrderStatusType.Open)
                        .Include(x => x.OriginalOrder)
                        .Include(x => x.ResponceOrder)
                        .ToListAsync();
                    Parallel.ForEach(allOpenedOrders, openedOrder =>
                    {
                        try
                        {
                            var exchange = _dictionaryModelsMapper.GetName<ExchangeModel>(openedOrder.ExchangeId);
                            var targetInstrument = _instrumentsNameMapper.GetExchangeInstrument(
                                _dictionaryModelsMapper.GetName<InstrumentModel>(openedOrder.InstrumentId), exchange);
                            _ordersWatcher.StartOrderTracking(new OrderIdentifier(openedOrder.OrderId, targetInstrument), exchange);
                            _allOrders.AddOrUpdate(openedOrder.OrderId, openedOrder, (s, model) => openedOrder);
                            _orderCancelationWatcher.AddToWatcher(openedOrder);
                        }
                        catch (Exception e)
                        {
                            _logger?.Log(SeverityLevel.Error, $"Failed to start tracking for order Id = {openedOrder.Id}!");
                            _logger?.Log(SeverityLevel.Error, e);
                        }
                    });
                }
            }
            catch (Exception e)
            {
                _logger?.Log(SeverityLevel.Error, "Failed to start tracking for orders!");
                _logger?.Log(SeverityLevel.Error, e);
                throw;
            }
        }
    }
}