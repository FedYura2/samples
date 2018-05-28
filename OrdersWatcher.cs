using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using xxx.xxx.Domain.Abstract;
using xxx.xxx.Domain.Models;
using xxx.xxx.Services.Configurations.Abstract;
using xxx.xxx.Services.Configurations.Helper;
using xxx.xxx.Services.Logging;

namespace xxx.xxx.Domain.Logics
{
    public interface IOrdersWatcher
    {
        void StartOrderTracking(OrderIdentifier orderIdentifier, string exchange);
        void StopOrderTracking(OrderIdentifier orderIdentifier, string exchange);
        event EventHandler<OrderStatusUpdateArgs> OnOrderStatusUpdated;
    }

    public class OrdersWatcher : IOrdersWatcher
    {
        private readonly IConfigurationsAdapter _configurationsAdapter;
        private readonly IExchangesManager _exchangesManager;
        private readonly ILogger _logger;

        private readonly ConcurrentDictionary<string, ConcurrentDictionary<string, OrderIdentifier>> _ordersToCheck;

        public event EventHandler<OrderStatusUpdateArgs> OnOrderStatusUpdated;

        #region Configurations

        private int _updateTimeForUpdatesOrdersInMs = 2000;

        #endregion

        public OrdersWatcher(IConfigurationsAdapter configurationsAdapter, IExchangesManager exchangesManager, ILogger logger)
        {
            _configurationsAdapter = configurationsAdapter;
            _exchangesManager = exchangesManager;
            _logger = logger ?? new DefaultLogger();
            _ordersToCheck = new ConcurrentDictionary<string, ConcurrentDictionary<string, OrderIdentifier>>();
            InitConfig();
            StartTracking();
        }

        public void StopOrderTracking(OrderIdentifier orderIdentifier, string exchange)
        {
            try
            {
                if (_ordersToCheck.TryGetValue(exchange, out ConcurrentDictionary<string, OrderIdentifier> collection) &&
                    collection.ContainsKey(orderIdentifier.Value) && collection.TryRemove(orderIdentifier.Value, out OrderIdentifier order))
                {
                    _logger?.Log(SeverityLevel.Debug, $"Order: {order}; Exchange: {exchange}. Removed from tracking service!");
                }
                else
                {
                    _logger?.Log(SeverityLevel.Debug, $"Order: {orderIdentifier}; Exchange: {exchange}. Not found! Can't removed from tracking service!");
                }
            }
            catch (Exception e)
            {
                _logger?.Log(SeverityLevel.Error, $"Failed to remove order: {orderIdentifier}; Exchange: {exchange} from tracking service!");
                _logger?.Log(SeverityLevel.Error, e);
            }
        }

        public void StartOrderTracking(OrderIdentifier orderIdentifier, string exchange)
        {
            try
            {
                var newCollection = new ConcurrentDictionary<string, OrderIdentifier>();
                newCollection.AddOrUpdate(orderIdentifier.Value, orderIdentifier, (s, identifier) => orderIdentifier);
                _ordersToCheck.AddOrUpdate(exchange, newCollection,
                    (s, list) =>
                    {
                        list.AddOrUpdate(orderIdentifier.Value, orderIdentifier,
                            (s1, identifier) => orderIdentifier);
                        return list;
                    });
                _logger?.Log(SeverityLevel.Debug, $"Add order: {orderIdentifier}; Exchange: {exchange}. To tracking service.");
            }
            catch (Exception e)
            {
                _logger?.Log(SeverityLevel.Error, $"Failed to add order: {orderIdentifier}; Exchange: {exchange}. To tracking service!");
                _logger?.Log(SeverityLevel.Error, e);
            }
        }


        private void InitConfig()
        {
            try
            {
                _configurationsAdapter.SubscribeToConfigurationChange(DomainLogicSettings.ORDERS_WATCHER_UPDATE_TIME_FOR_UPDATES_ORDERS,
                    configuration =>
                    {
                        if (configuration.OldValue != configuration.NewValue && int.TryParse(configuration.NewValue, out int updateTime))
                        {
                            _updateTimeForUpdatesOrdersInMs = updateTime;
                        }
                    });
                var frequentUpdateTime = _configurationsAdapter.GetConfiguration(DomainLogicSettings
                    .ORDERS_WATCHER_UPDATE_TIME_FOR_UPDATES_ORDERS);
                if (!string.IsNullOrEmpty(frequentUpdateTime) && int.TryParse(frequentUpdateTime, out int value))
                {
                    _updateTimeForUpdatesOrdersInMs = value;
                }
            }
            catch (Exception e)
            {
                _logger?.Log(SeverityLevel.Error, e);
            }
        }

        private void StartTracking()
        {
            Task.Factory.StartNew(() =>
            {
                var allExchanges = _ordersToCheck.Keys.ToList();
                while (true)
                {
                    var allExchangesNew = _ordersToCheck.Keys.ToList();
                    if (!allExchanges.SequenceEqual(allExchangesNew))
                    {
                        var newExchanges = allExchangesNew.Where(x => allExchanges.All(e => e != x)).ToList();
                        foreach (var newExchange in newExchanges)
                        {
                            StartTrackingFrequentOrdersForExchange(newExchange);
                        }
                    }
                    allExchanges = allExchangesNew;
                    Thread.Sleep(1000);
                }
            }, TaskCreationOptions.LongRunning);
        }

        private void StartTrackingFrequentOrdersForExchange(string newExchange)
        {
            Task.Factory.StartNew(async () =>
            {
                var lastCkeck = DateTime.UtcNow;
                while (true)
                {
                    IExchange exchange;
                    try
                    {
                        exchange = _exchangesManager.GetExchangeByName(newExchange);

                        if (_ordersToCheck.TryGetValue(newExchange, out ConcurrentDictionary<string, OrderIdentifier> collection))
                        {
                            await Task.WhenAll(collection.Values.ToList()
                                .Select(async x =>
                                {
                                    try
                                    {
                                        var state = await exchange.GetOrderStatusAsync(x);
                                        Volatile.Read(ref OnOrderStatusUpdated)?.Invoke(this, new OrderStatusUpdateArgs()
                                        {
                                            OrderIdentifier = x,
                                            OrderStatus = state,
                                            Exchange = newExchange
                                        });
                                    }
                                    catch (Exception e)
                                    {
                                        _logger?.Log(SeverityLevel.Error, $"Failed to update state for order: {x}; Exchange: {newExchange}!");
                                        _logger?.Log(SeverityLevel.Error, e);
                                    }
                                }));
                        }
                    }
                    catch (Exception e)
                    {
                        _logger?.Log(SeverityLevel.Info, $"Can't get exchange {newExchange}!");
                        _logger?.Log(SeverityLevel.Info, e);
                    }
                    var diff = (DateTime.UtcNow - lastCkeck).TotalMilliseconds;
                    if (diff < _updateTimeForUpdatesOrdersInMs)
                    {
                        Thread.Sleep(TimeSpan.FromMilliseconds(_updateTimeForUpdatesOrdersInMs - diff));
                    }
                    lastCkeck = DateTime.UtcNow;
                }
            }, TaskCreationOptions.LongRunning);
        }
    }
}