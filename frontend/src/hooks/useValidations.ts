import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { validationApi, aiApi } from '@/services/api';
import { useStore } from '@/store';
import toast from 'react-hot-toast';

export const useSubmitValidation = () => {
  const queryClient = useQueryClient();
  const addValidationRun = useStore((state) => state.addValidationRun);
  
  return useMutation({
    mutationFn: validationApi.submit,
    onSuccess: (data) => {
      addValidationRun({
        id: data.validation_id,
        status: 'pending',
        data_source_id: '',
        validation_mode: '',
        target_path: '',
        total_rules: 0,
        passed_rules: 0,
        failed_rules: 0,
        records_processed: 0,
      });
      queryClient.invalidateQueries({ queryKey: ['validations'] });
      toast.success('Validation started successfully');
      return data;
    },
    onError: (error: any) => {
      toast.error(error.response?.data?.detail || 'Failed to start validation');
    },
  });
};

export const useValidationStatus = (id: string | null) => {
  const updateValidationRun = useStore((state) => state.updateValidationRun);
  
  return useQuery({
    queryKey: ['validation', id],
    queryFn: async () => {
      if (!id) return null;
      const data = await validationApi.getStatus(id);
      updateValidationRun(id, data);
      return data;
    },
    refetchInterval: (data) => {
      if (data?.status === 'running' || data?.status === 'pending') {
        return 2000; // Poll every 2 seconds while running
      }
      return false;
    },
    enabled: !!id,
  });
};

export const useValidationResults = (id: string | null) => {
  return useQuery({
    queryKey: ['validation', id, 'results'],
    queryFn: async () => {
      if (!id) return null;
      return validationApi.getResults(id);
    },
    enabled: !!id,
  });
};

export const useRecommendRules = () => {
  return useMutation({
    mutationFn: ({ data_source_id, target_path, sample_size }: {
      data_source_id: string;
      target_path: string;
      sample_size?: number;
    }) => aiApi.recommendRules(data_source_id, target_path, sample_size),
    onSuccess: (data) => {
      toast.success(`Generated ${data.rules?.length || 0} AI-recommended rules`);
      return data;
    },
    onError: (error: any) => {
      toast.error(error.response?.data?.detail || 'Failed to generate rules');
    },
  });
};

export const useAnalyzeData = () => {
  return useMutation({
    mutationFn: ({ data_source_id, target_path }: {
      data_source_id: string;
      target_path: string;
    }) => aiApi.analyze(data_source_id, target_path),
    onSuccess: (data) => {
      toast.success('Data analysis completed');
      return data;
    },
    onError: (error: any) => {
      toast.error(error.response?.data?.detail || 'Failed to analyze data');
    },
  });
};
