const PageContainer = ({ title, description, children }) => (
  <div>
    <title>{title}</title>
    <meta name="description" content={description} />
    {children}
  </div>
);

export default PageContainer;
