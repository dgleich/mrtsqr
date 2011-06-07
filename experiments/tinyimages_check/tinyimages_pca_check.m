%% Check tiny images PCA from hadoop

%% 
% First run the tinyimages pca set to load it's PCs
tinyimages_pca
%%
pcaR =load('~/remote/nebula/publications/tsqr/experiments/tinyimages/pca-R.tmat');
%%
[U,Shadoop,Vhadoop] = svd(pcaR);
figure(2);
imdisp(reshape(Vhadoop(:,1:16),32,32,1,16), cmap,'Size',[4 4],'Border',0.005)

%%
possign = @(A) A*diag(sign(A(1,:)));

%%
figure(4);
plot([diag(S), diag(Shadoop), diag(Sqr)])


%%
figure(4);
plot(sum(abs(possign(V)-possign(Vhadoop))))
%%
% difference in V
figure(4);
plot(sum(abs(possign(Vqr)-possign(Vhadoop))))
%%2
% difference in R
pcaR2 = diag(sign(diag(pcaR)))*pcaR;
figure(4);
plot(sum(abs(pcaR2-R),2))
