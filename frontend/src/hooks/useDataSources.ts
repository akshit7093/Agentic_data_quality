import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { dataSourceApi } from '@/services/api';
import { useStore } from '@/store';
import toast from 'react-hot-toast';

export const useDataSources = () => {
  const setDataSources = useStore((state) => state.setDataSources);
  
  return useQuery({
    queryKey: ['dataSources'],
    queryFn: async () => {
      const data = await dataSourceApi.getAll();
      setDataSources(data);
      return data;
    },
  });
};

export const useCreateDataSource = () => {
  const queryClient = useQueryClient();
  const addDataSource = useStore((state) => state.addDataSource);
  
  return useMutation({
    mutationFn: dataSourceApi.create,
    onSuccess: (data) => {
      addDataSource(data);
      queryClient.invalidateQueries({ queryKey: ['dataSources'] });
      toast.success('Data source created successfully');
    },
    onError: (error: any) => {
      toast.error(error.response?.data?.detail || 'Failed to create data source');
    },
  });
};

export const useUpdateDataSource = () => {
  const queryClient = useQueryClient();
  const updateDataSource = useStore((state) => state.updateDataSource);
  
  return useMutation({
    mutationFn: ({ id, data }: { id: string; data: any }) =>
      dataSourceApi.update(id, data),
    onSuccess: (data, variables) => {
      updateDataSource(variables.id, data);
      queryClient.invalidateQueries({ queryKey: ['dataSources'] });
      toast.success('Data source updated successfully');
    },
    onError: (error: any) => {
      toast.error(error.response?.data?.detail || 'Failed to update data source');
    },
  });
};

export const useDeleteDataSource = () => {
  const queryClient = useQueryClient();
  const removeDataSource = useStore((state) => state.removeDataSource);
  
  return useMutation({
    mutationFn: dataSourceApi.delete,
    onSuccess: (_, id) => {
      removeDataSource(id);
      queryClient.invalidateQueries({ queryKey: ['dataSources'] });
      toast.success('Data source deleted successfully');
    },
    onError: (error: any) => {
      toast.error(error.response?.data?.detail || 'Failed to delete data source');
    },
  });
};

export const useTestConnection = () => {
  return useMutation({
    mutationFn: dataSourceApi.testConnection,
    onSuccess: (data) => {
      if (data.success) {
        toast.success('Connection successful!');
      } else {
        toast.error(data.message);
      }
    },
    onError: (error: any) => {
      toast.error(error.response?.data?.detail || 'Connection test failed');
    },
  });
};
