    // Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System;
using System.Globalization;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Azure.WebJobs.Host.Lease;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;

namespace Microsoft.Azure.WebJobs.Script
{
    internal sealed class BlobLeaseManager : IDisposable
    {
        internal const string LockBlobName = "host";
        internal const string HostContainerName = "azure-webjobs-hosts";

        private readonly Timer _timer;
        private readonly TimeSpan _leaseTimeout;
        private readonly TimeSpan _renewalInterval;
        private readonly TimeSpan _leaseRetryInterval;
        private readonly TraceWriter _traceWriter;
        private readonly string _hostId;
        private readonly string _instanceId;
        private ICloudBlob _lockBlob; // FIXME: not needed anymore
        private LeaseDefinition _leaseDefinition;
        private string _leaseId;
        private bool _disposed;
        private bool _processingLease;
        private DateTime _lastRenewal;
        private TimeSpan _lastRenewalLatency;
        private ILeaseProxy _leaseProxy;
        private string _accountName;

        //FIXME: remove this ctor
        internal BlobLeaseManager(ICloudBlob lockBlob, TimeSpan leaseTimeout, string hostId, string instanceId, TraceWriter traceWriter, TimeSpan? renewalInterval = null)
        {
            _lockBlob = lockBlob;
            _leaseTimeout = leaseTimeout;
            _traceWriter = traceWriter;
            _hostId = hostId;
            _instanceId = instanceId;

            // Renew the lease three seconds before it expires
            _renewalInterval = renewalInterval ?? leaseTimeout.Add(TimeSpan.FromSeconds(-3));

            // Attempt to acquire a lease every 5 seconds
            _leaseRetryInterval = TimeSpan.FromSeconds(5);

            _timer = new Timer(ProcessLeaseTimerTick, null, TimeSpan.Zero, _leaseRetryInterval);
        }

        //FIXME: consider combining multiple params into a single leasedef param
        internal BlobLeaseManager(ILeaseProxy leaseProxy, string accountName, LeaseDefinition leaseDefinition, TimeSpan leaseTimeout, string hostId, string instanceId, TraceWriter traceWriter, TimeSpan? renewalInterval = null)
        {
            _leaseProxy = leaseProxy;
            _accountName = accountName;
            _leaseDefinition = leaseDefinition;
            _leaseTimeout = leaseTimeout;
            _traceWriter = traceWriter;
            _hostId = hostId;
            _instanceId = instanceId;

            // Renew the lease three seconds before it expires
            _renewalInterval = renewalInterval ?? leaseTimeout.Add(TimeSpan.FromSeconds(-3));

            // Attempt to acquire a lease every 5 seconds
            _leaseRetryInterval = TimeSpan.FromSeconds(5);

            _timer = new Timer(ProcessLeaseTimerTick, null, TimeSpan.Zero, _leaseRetryInterval);
        }

        public event EventHandler HasLeaseChanged;

        public bool HasLease => _leaseId != null;

        public string LeaseId
        {
            get
            {
                return _leaseId;
            }
            private set
            {
                string previousId = _leaseId;
                _leaseId = value;

                if (string.Compare(previousId, _leaseId, StringComparison.OrdinalIgnoreCase) != 0)
                {
                    OnHasLeaseChanged();
                }
            }
        }

        private void OnHasLeaseChanged() => HasLeaseChanged?.Invoke(this, EventArgs.Empty);

        public static Task<BlobLeaseManager> CreateAsync(string s, TimeSpan leaseTimeout, string hostId,
            string instanceId, TraceWriter traceWriter)
        {
            // FIXME: method not needed. just adding to keep tests compiling
            return Task.FromResult<BlobLeaseManager>(null);
        }

        public static BlobLeaseManager Create(ILeaseProxy leaseProxy, string accountName, TimeSpan leaseTimeout, string hostId, string instanceId, TraceWriter traceWriter)
        {
            if (leaseTimeout.TotalSeconds < 15 || leaseTimeout.TotalSeconds > 60)
            {
                throw new ArgumentOutOfRangeException(nameof(leaseTimeout), $"The {nameof(leaseTimeout)} should be between 15 and 60 seconds");
            }

            var manager = new BlobLeaseManager(leaseProxy, accountName, null, leaseTimeout, hostId, instanceId, traceWriter);
            return manager;
        }

        public static BlobLeaseManager Create(string accountConnectionString, TimeSpan leaseTimeout, string hostId, string instanceId, TraceWriter traceWriter)
        {
            // FIXME
            // return CreateAsync(null, null, leaseTimeout, hostId, instanceId, traceWriter).GetAwaiter().GetResult();
            return null;
        }

        private void ProcessLeaseTimerTick(object state)
        {
            if (_processingLease)
            {
                return;
            }

            _processingLease = true;

            AcquireOrRenewLeaseAsync()
                .ContinueWith(t =>
                {
                    if (t.IsFaulted)
                    {
                        t.Exception.Handle(e =>
                        {
                            ProcessLeaseError(e.Message);
                            return true;
                        });
                    }

                    _processingLease = false;
                }, TaskContinuationOptions.ExecuteSynchronously);
        }

        private async Task AcquireOrRenewLeaseAsync()
        {
            try
            {
                LeaseDefinition leaseDefinition = new LeaseDefinition
                {
                    AccountName = _accountName,
                    Namespace = HostContainerName,
                    Category = null,
                    LockId = _hostId,
                    Period = _leaseTimeout
                };

                DateTime requestStart = DateTime.UtcNow;
                if (HasLease)
                {
                    leaseDefinition.LeaseId = LeaseId;
                    await _leaseProxy.RenewLeaseAsync(leaseDefinition, CancellationToken.None);
                    _lastRenewal = DateTime.UtcNow;
                    _lastRenewalLatency = _lastRenewal - requestStart;
                }
                else
                {
                    leaseDefinition.LeaseId = _instanceId;
                    LeaseId = await _leaseProxy.AcquireLeaseAsync(leaseDefinition, CancellationToken.None);
                    _lastRenewal = DateTime.UtcNow;
                    _lastRenewalLatency = _lastRenewal - requestStart;

                    _traceWriter.Info($"Host lock lease acquired by instance ID '{_instanceId}'.");

                    // We've successfully acquired the lease, change the timer to use our renewal interval
                    SetTimerInterval(_renewalInterval);
                }
            }
            catch (LeaseException exc)
            {                
                if (exc.FailureReason == LeaseFailureReason.Conflict)
                {
                    // FIXME: update comment
                    // If we did not have the lease already, a 409 indicates that another host had it. This is 
                    // normal and does not warrant any logging.

                    if (HasLease)
                    {
                        // The lease was 'stolen'. Log details for debugging.
                        string lastRenewalFormatted = _lastRenewal.ToString("yyyy-MM-ddTHH:mm:ss.FFFZ", CultureInfo.InvariantCulture);
                        int millisecondsSinceLastSuccess = (int)(DateTime.UtcNow - _lastRenewal).TotalMilliseconds;
                        int lastRenewalMilliseconds = (int)_lastRenewalLatency.TotalMilliseconds;
                        ProcessLeaseError($"Another host has acquired the lease. The last successful renewal completed at {lastRenewalFormatted} ({millisecondsSinceLastSuccess} milliseconds ago) with a duration of {lastRenewalMilliseconds} milliseconds.");
                    }
                }

                ProcessLeaseError($"Server error {exc}."); // FIXME: make sure this logs details as expected. 
                throw;
            }
        }

        internal static string GetBlobName(string hostId) => $"locks/{hostId}/{LockBlobName}";

        private void ProcessLeaseError(string reason)
        {
            if (HasLease)
            {
                ResetLease();

                _traceWriter.Info($"Failed to renew host lock lease: {reason}");
            }
            else
            {
                _traceWriter.Verbose($"Host instance '{_instanceId}' failed to acquire host lock lease: {reason}");
            }
        }

        private static async Task<ICloudBlob> GetLockBlobAsync(string accountConnectionString, string blobName)
        {
            CloudStorageAccount account = CloudStorageAccount.Parse(accountConnectionString);
            CloudBlobClient client = account.CreateCloudBlobClient();

            return await GetLockBlobAsync(client, blobName);
        }

        private static async Task<ICloudBlob> GetLockBlobAsync(CloudBlobClient client, string blobName)
        {
            var container = client.GetContainerReference(HostContainerName);

            try
            {
                await container.CreateIfNotExistsAsync();
            }
            catch (StorageException exc)
            when (exc.RequestInformation.HttpStatusCode == 409 && string.Compare("ContainerBeingDeleted", exc.RequestInformation.ExtendedErrorInformation?.ErrorCode) == 0)
            {
                throw new StorageException("The host container is pending deletion and currently inaccessible.");
            }

            CloudBlockBlob blob = container.GetBlockBlobReference(blobName);
            if (!await blob.ExistsAsync())
            {
                try
                {
                    await blob.UploadFromStreamAsync(new MemoryStream());
                }
                catch (StorageException exc)
                when (exc.RequestInformation.HttpStatusCode == 412 || exc.RequestInformation.HttpStatusCode == 409)
                {
                    // The blob already exists or a lease has already been acquired.
                }
            }

            return blob;
        }

        private void ResetLease()
        {
            LeaseId = null;
            SetTimerInterval(_leaseRetryInterval);
        }

        private void SetTimerInterval(TimeSpan interval, TimeSpan? dueTimeout = null)
        {
            _timer.Change(dueTimeout ?? interval, interval);
        }

        private void TryReleaseLeaseIfOwned()
        {
            try
            {
                if (HasLease)
                {
                    LeaseDefinition leaseDefinition = new LeaseDefinition
                    {
                        AccountName = _accountName,
                        Namespace = HostContainerName,
                        Category = null,
                        LockId = _hostId,
                        LeaseId = LeaseId,
                        Period = _leaseTimeout
                    };
                    _leaseProxy.ReleaseLeaseAsync(leaseDefinition, CancellationToken.None).GetAwaiter().GetResult(); // FIXME: .Result
                    _traceWriter.Verbose($"Host instance '{_instanceId}' released lock lease.");
                }
            }
            catch (Exception)
            {
                // Best effort, the lease will expire if we fail to release it.
            }
        }

        private void Dispose(bool disposing)
        {
            if (!_disposed)
            {
                if (disposing)
                {
                    _timer.Dispose();

                    TryReleaseLeaseIfOwned();
                }

                _disposed = true;
            }
        }

        public void Dispose()
        {
            Dispose(true);
        }
    }
}
