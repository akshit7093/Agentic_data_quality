import { BrowserRouter, Routes, Route } from 'react-router-dom';
import Layout from '@/components/Layout';
import Dashboard from '@/pages/Dashboard';
import DataSources from '@/pages/DataSources';
import Validations from '@/pages/Validations';
import NewValidation from '@/pages/NewValidation';
import ValidationDetail from '@/pages/ValidationDetail';
import Rules from '@/pages/Rules';
import Settings from '@/pages/Settings';

function App() {
  return (
    <BrowserRouter>
      <Layout>
        <Routes>
          <Route path="/" element={<Dashboard />} />
          <Route path="/datasources" element={<DataSources />} />
          <Route path="/validations" element={<Validations />} />
          <Route path="/validations/new" element={<NewValidation />} />
          <Route path="/validations/:id" element={<ValidationDetail />} />
          <Route path="/rules" element={<Rules />} />
          <Route path="/settings" element={<Settings />} />
        </Routes>
      </Layout>
    </BrowserRouter>
  );
}

export default App;
