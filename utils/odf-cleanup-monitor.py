#!/usr/bin/env python3
"""Monitor ODF cleanup jobs in OpenShift and report failures.

This script checks all jobs in the 'cleanup' namespace, analyzes their logs,
and reports any failed cleanup operations with GUID and error details.

Author: yvarbev@redhat.com, gh:@yordangit
Version: 25.08.04
"""

import re
import csv
import sys
import urllib3
from datetime import datetime
from typing import List, Dict, Optional, Tuple
from kubernetes import client, config

# Disable SSL warnings for self-signed certificates
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class CleanupJobMonitor:
    """Monitor and analyze ODF cleanup jobs for failures"""
    
    def __init__(self, namespace: str = "cleanup", debug: bool = False):
        self.namespace = namespace
        self.debug = debug
        self.v1 = None
        self.batch_v1 = None
        self._setup_k8s_client()
        
    def _setup_k8s_client(self):
        """Setup Kubernetes API client"""
        try:
            # Load kubeconfig (works both in-cluster and external)
            try:
                config.load_incluster_config()
                if self.debug:
                    print("[v] Using in-cluster Kubernetes config")
            except config.ConfigException:
                config.load_kube_config()
                if self.debug:
                    print("[v] Using kubeconfig file")
                    
            # Create API clients
            self.v1 = client.CoreV1Api()
            self.batch_v1 = client.BatchV1Api()
            
            if self.debug:
                print(f"[v] Connected to Kubernetes cluster")
                
        except Exception as e:
            print(f"Error setting up Kubernetes client: {e}")
            print("Make sure kubeconfig is valid or you're running in a cluster with proper RBAC")
            sys.exit(1)
        
    def get_cleanup_jobs(self) -> List:
        """Get all jobs in the cleanup namespace"""
        try:
            jobs = self.batch_v1.list_namespaced_job(namespace=self.namespace)
            if self.debug:
                print(f"[v] Found {len(jobs.items)} jobs in namespace '{self.namespace}'")
            return jobs.items
        except Exception as e:
            print(f"Error getting jobs: {e}")
            return []
    
    def get_job_pods(self, job_name: str) -> List:
        """Get pods for a specific job"""
        try:
            label_selector = f'job-name={job_name}'
            pods = self.v1.list_namespaced_pod(
                namespace=self.namespace,
                label_selector=label_selector
            )
            return pods.items
        except Exception as e:
            if self.debug:
                print(f"Error getting pods for job {job_name}: {e}")
            return []
    
    def get_job_logs(self, job_name: str) -> Optional[str]:
        """Get logs for a specific job"""
        try:
            # Get pods for this job
            pods = self.get_job_pods(job_name)
            if not pods:
                return None
                
            # Get logs from the first pod (jobs typically have one pod)
            pod_name = pods[0].metadata.name
            logs = self.v1.read_namespaced_pod_log(
                name=pod_name,
                namespace=self.namespace,
                pretty=True
            )
            return logs
        except Exception:
            # Pod might not have started or logs might not be available
            return None
    
    def extract_guid_from_job_name(self, job_name: str) -> Optional[str]:
        """Extract GUID from job name - assuming it contains the lab GUID"""
        # Try common patterns for cleanup job names
        patterns = [
            r'cleanup-ceph-sandbox-([a-z0-9]+)(?:-\d+)?-ocp4-cluster',  # cleanup-ceph-sandbox-{GUID}[-{num}]-ocp4-cluster-{num}
            r'cleanup-([a-z0-9]+)(?:-|$)',  # cleanup-{GUID}-... (fallback)
            r'lab-([a-z0-9]+)-cleanup',     # lab-{GUID}-cleanup
            r'([a-z0-9]+)-cleanup',         # {GUID}-cleanup
        ]
        
        for pattern in patterns:
            match = re.search(pattern, job_name)
            if match:
                return match.group(1)
        return None
    
    def extract_guid_from_logs(self, logs: str) -> Optional[str]:
        """Extract GUID from log content"""
        # Look for volume names in processing lines
        volume_pattern = r'ocp4-cluster-([a-z0-9]+)-[a-f0-9-]+'
        match = re.search(volume_pattern, logs)
        if match:
            return match.group(1)
            
        # Look for LAB GUID in configuration output
        config_pattern = r'LAB GUID:\s*([a-z0-9]+)'
        match = re.search(config_pattern, logs)
        if match:
            return match.group(1)
            
        return None
    
    def check_namespace_exists(self, guid: str) -> bool:
        """Check if a namespace containing the GUID still exists"""
        if not guid or guid == 'Unknown':
            return False
            
        try:
            # List all namespaces
            namespaces = self.v1.list_namespace()
            
            # Check if any namespace contains the GUID
            for ns in namespaces.items:
                ns_name = ns.metadata.name
                if guid in ns_name:
                    if self.debug:
                        print(f"    Found namespace containing GUID {guid}: {ns_name}")
                    return True
                    
            return False
            
        except Exception as e:
            if self.debug:
                print(f"    Error checking namespaces for GUID {guid}: {e}")
            return False
    
    def parse_error_details(self, logs: str) -> List[Tuple[str, str]]:
        """Parse error details from logs"""
        errors = []
        
        # Find ERROR lines
        error_pattern = r'ERROR:\s*(.+)'
        error_matches = re.findall(error_pattern, logs)
        
        # Find FAILED lines  
        failed_pattern = r'FAILED:\s*(.+)'
        failed_matches = re.findall(failed_pattern, logs)
        
        # Add errors
        for error in error_matches:
            errors.append(("ERROR", error.strip()))
            
        # Add failures
        for failure in failed_matches:
            errors.append(("FAILED", failure.strip()))
            
        # Check for warning about items still failing
        warning_pattern = r'Warning:\s*(\d+)\s*items still failed'
        warning_match = re.search(warning_pattern, logs)
        if warning_match:
            count = warning_match.group(1)
            errors.append(("WARNING", f"{count} items still failed after trash purge and retry"))
            
        return errors
    
    def is_job_successful(self, logs: str) -> bool:
        """Check if job was successful based on logs"""
        if not logs:
            return False
            
        # Look for success indicators
        success_patterns = [
            r'SUCCESS:\s*No objects with GUID found in pool',
            r'Cleanup completed successfully for LAB GUID',
            r'All items cleaned up successfully'
        ]
        
        for pattern in success_patterns:
            if re.search(pattern, logs):
                return True
                
        return False
    
    def get_job_status(self, job) -> str:
        """Get job status from Kubernetes job object"""
        if not job.status or not job.status.conditions:
            return 'Running'
            
        for condition in job.status.conditions:
            if condition.type == 'Complete' and condition.status == 'True':
                return 'Complete'
            elif condition.type == 'Failed' and condition.status == 'True':
                return 'Failed'
        return 'Running'
    
    def analyze_job(self, job) -> Optional[Dict]:
        """Analyze a single job for failures"""
        job_name = job.metadata.name
        job_status = self.get_job_status(job)
        
        # Get completion time
        completion_time = None
        if job.status and job.status.conditions:
            for condition in job.status.conditions:
                if condition.type in ['Complete', 'Failed']:
                    completion_time = str(condition.last_transition_time) if condition.last_transition_time else 'Unknown'
                    break
        
        # Get logs
        logs = self.get_job_logs(job_name)
        if not logs:
            guid = self.extract_guid_from_job_name(job_name) or 'Unknown'
            return {
                'job_name': job_name,
                'guid': guid,
                'project': self.check_namespace_exists(guid),
                'status': job_status,
                'completion_time': completion_time,
                'error_type': 'NO_LOGS',
                'error_reason': 'Unable to retrieve job logs'
            }
        
        # Check if successful
        if self.is_job_successful(logs):
            return None  # Job was successful
        
        # Job failed or had issues - extract details
        guid = self.extract_guid_from_logs(logs) or self.extract_guid_from_job_name(job_name) or 'Unknown'
        error_details = self.parse_error_details(logs)
        
        if not error_details:
            # No specific errors found, but job didn't show success
            error_details = [('UNKNOWN', 'Job completed without success message')]
        
        # Return failure info (use first error as primary)
        primary_error = error_details[0]
        return {
            'job_name': job_name,
            'guid': guid,
            'project': self.check_namespace_exists(guid),
            'status': job_status,
            'completion_time': completion_time,
            'error_type': primary_error[0],
            'error_reason': primary_error[1],
            'all_errors': error_details
        }
    
    def monitor_jobs(self) -> List[Dict]:
        """Monitor all cleanup jobs and return failed ones"""
        print(f"Checking cleanup jobs in namespace: {self.namespace}")
        
        jobs = self.get_cleanup_jobs()
        if not jobs:
            print("No jobs found or unable to retrieve jobs")
            return []
        
        print(f"Found {len(jobs)} jobs to analyze")
        failed_jobs = []
        
        for job in jobs:
            job_name = job.metadata.name
            if self.debug:
                print(f"Analyzing job: {job_name}")
            
            failure_info = self.analyze_job(job)
            if failure_info:
                failed_jobs.append(failure_info)
                if self.debug:
                    print(f"  [x] FAILED: {failure_info['guid']} - {failure_info['error_reason']}")
            else:
                if self.debug:
                    print(f"  [v] SUCCESS")
        
        # Print summary
        total_jobs = len(jobs)
        failed_count = len(failed_jobs)
        success_count = total_jobs - failed_count
        print(f"\nSummary: {total_jobs} jobs analyzed, {failed_count} failures, {success_count} success")
        
        return failed_jobs
    
    def generate_csv_report(self, failed_jobs: List[Dict], filename: str = None):
        """Generate CSV report of failed jobs"""
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"cleanup_failures_{timestamp}.csv"
        
        with open(filename, 'w', newline='') as csvfile:
            fieldnames = ['job_name', 'guid', 'project', 'status', 'completion_time', 'error_type', 'error_reason']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            
            writer.writeheader()
            for job in failed_jobs:
                writer.writerow({
                    'job_name': job['job_name'],
                    'guid': job['guid'],
                    'project': 'Active' if job['project'] else 'Deleted',
                    'status': job['status'],
                    'completion_time': job['completion_time'],
                    'error_type': job['error_type'],
                    'error_reason': job['error_reason']
                })
        
        print(f"CSV report generated: {filename}")
    
    def print_summary_report(self, failed_jobs: List[Dict]):
        """Print a summary report to console"""
        if not failed_jobs:
            print("\n[v] All cleanup jobs completed successfully!")
            return
        
        print(f"\n[x] Found {len(failed_jobs)} failed cleanup jobs:")
        print("=" * 80)
        
        for job in failed_jobs:
            print(f"GUID: {job['guid']}")
            print(f"Job: {job['job_name']}")
            print(f"Project: {'Active' if job['project'] else 'Deleted'}")
            print(f"Status: {job['status']}")
            print(f"Completed: {job['completion_time']}")
            print(f"Error: {job['error_type']} - {job['error_reason']}")
            
            # Show all errors if multiple
            if len(job.get('all_errors', [])) > 1:
                print("Additional errors:")
                for error_type, error_reason in job['all_errors'][1:]:
                    print(f"  - {error_type}: {error_reason}")
            
            print("-" * 40)


def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Monitor ODF cleanup jobs for failures')
    parser.add_argument('--namespace', '-n', default='cleanup', 
                       help='Namespace to monitor (default: cleanup)')
    parser.add_argument('--csv', '-c', 
                       help='Generate CSV report with specified filename')
    parser.add_argument('--format', '-f', choices=['console', 'csv', 'both'], 
                       default='both', help='Output format (default: both)')
    parser.add_argument('--debug', '-d', action='store_true',
                       help='Enable debug output')
    
    args = parser.parse_args()
    
    monitor = CleanupJobMonitor(namespace=args.namespace, debug=args.debug)
    failed_jobs = monitor.monitor_jobs()
    
    if args.format in ['console', 'both']:
        monitor.print_summary_report(failed_jobs)
    
    if args.format in ['csv', 'both'] or args.csv:
        monitor.generate_csv_report(failed_jobs, args.csv)
    
    # Exit with error code if failures found
    sys.exit(1 if failed_jobs else 0)


if __name__ == "__main__":
    main()